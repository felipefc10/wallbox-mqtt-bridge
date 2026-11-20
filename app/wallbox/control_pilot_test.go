package wallbox

import "testing"

func TestControlPilotLetter(t *testing.T) {
	cases := map[int]string{
		0:    "", // no source yet: nothing is published
		0xA1: "A",
		0xA2: "A",
		0xB1: "B",
		0xB2: "B",
		0xC1: "C",
		0xC2: "C",
		0xC3: "C",
		0xE:  "E",
		0xF:  "F",
		0x14: "", // unknown codes are not guessed
		0x1A: "",
	}
	for cp, want := range cases {
		if got := controlPilotLetter(cp); got != want {
			t.Errorf("controlPilotLetter(%#x) = %q, want %q", cp, got, want)
		}
	}
}
