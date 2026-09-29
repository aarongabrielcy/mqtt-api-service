package mqtt

import (
	"testing"
	"time"
)

var testPolicy = ReconnectPolicy{
	InitialDelay:         2 * time.Second,
	MaxDelay:             5 * time.Minute,
	StableConnection:     60 * time.Second,
	InstabilityThreshold: 3,
}

func TestDefaultReconnectPolicy_IsFinite(t *testing.T) {
	p := DefaultReconnectPolicy
	if p.InitialDelay <= 0 || p.MaxDelay < p.InitialDelay || p.StableConnection <= 0 || p.InstabilityThreshold <= 0 {
		t.Fatalf("default policy must have explicit finite positive bounds, got %+v", p)
	}
	if p.InitialDelay != 2*time.Second || p.MaxDelay != 5*time.Minute || p.StableConnection != 60*time.Second || p.InstabilityThreshold != 3 {
		t.Fatalf("default policy changed unexpectedly: %+v", p)
	}
}

func TestBackoff_StableLossStartsAtInitialDelay(t *testing.T) {
	b := newReconnectBackoff(testPolicy)

	delay, shortLived, streak := b.connectionLost(10 * time.Minute)

	if delay != testPolicy.InitialDelay || shortLived || streak != 0 {
		t.Fatalf("got delay=%s shortLived=%v streak=%d", delay, shortLived, streak)
	}
}

func TestBackoff_FailedAttemptsDoubleUpToMax(t *testing.T) {
	b := newReconnectBackoff(testPolicy)

	got := []time.Duration{}
	d, _, _ := b.connectionLost(10 * time.Minute)
	got = append(got, d)
	for i := 0; i < 12; i++ {
		got = append(got, b.attemptFailed())
	}

	want := []time.Duration{
		2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		32 * time.Second, 64 * time.Second, 128 * time.Second, 256 * time.Second,
		5 * time.Minute, 5 * time.Minute, 5 * time.Minute, 5 * time.Minute, 5 * time.Minute,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delay[%d] = %s, want %s (all: %v)", i, got[i], want[i], got)
		}
	}
}

// Ciclos rápidos conecta->corte (p. ej. Client ID duplicado) no reinician
// el backoff: el retraso nunca baja, siempre está en [Initial, Max] y
// converge a MaxDelay, así que no hay loop apretado.
func TestBackoff_RapidConnectLossCyclesCannotTightLoop(t *testing.T) {
	b := newReconnectBackoff(testPolicy)

	const cycles = 1000
	var prev, total time.Duration
	for i := 0; i < cycles; i++ {
		d, shortLived, streak := b.connectionLost(3 * time.Second)
		if !shortLived || streak != i+1 {
			t.Fatalf("cycle %d: shortLived=%v streak=%d", i, shortLived, streak)
		}
		if d < testPolicy.InitialDelay || d > testPolicy.MaxDelay {
			t.Fatalf("cycle %d: delay %s out of bounds", i, d)
		}
		if d < prev {
			t.Fatalf("cycle %d: delay decreased from %s to %s during rapid churn", i, prev, d)
		}
		prev = d
		total += d
	}

	if prev != testPolicy.MaxDelay {
		t.Fatalf("rapid churn must converge to MaxDelay, got %s", prev)
	}

	// Cota de ritmo: tras saturar, cada ciclo espera MaxDelay, así que 1000
	// ciclos no pueden ocurrir en menos de ~ (1000-9)*MaxDelay.
	if min := time.Duration(cycles-9) * testPolicy.MaxDelay; total < min {
		t.Fatalf("total wait %s below bound %s", total, min)
	}
}

func TestBackoff_StableConnectionResetsBackoffAndStreak(t *testing.T) {
	b := newReconnectBackoff(testPolicy)

	for i := 0; i < 5; i++ {
		b.connectionLost(time.Second)
	}
	if !b.unstable() {
		t.Fatal("expected unstable after 5 short-lived connections")
	}

	d, shortLived, streak := b.connectionLost(testPolicy.StableConnection)
	if d != testPolicy.InitialDelay || shortLived || streak != 0 || b.unstable() {
		t.Fatalf("stable connection must reset: delay=%s shortLived=%v streak=%d unstable=%v", d, shortLived, streak, b.unstable())
	}
}

func TestBackoff_InstabilityThreshold(t *testing.T) {
	b := newReconnectBackoff(testPolicy)

	for i := 1; i <= testPolicy.InstabilityThreshold; i++ {
		b.connectionLost(time.Second)
		if want := i >= testPolicy.InstabilityThreshold; b.unstable() != want {
			t.Fatalf("after %d short-lived: unstable=%v, want %v", i, b.unstable(), want)
		}
	}
}
