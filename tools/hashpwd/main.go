// Command hashpwd verifies a bcrypt hash against a candidate password and
// prints a freshly generated hash (dev seeding helper, never shipped).
package main

import (
	"fmt"
	"os"

	"golang.org/x/crypto/bcrypt"
)

func main() {
	seed := "$2a$10$3ZnjZIeHxeDM1sVdi6J.kezWSVRX53tVt5ucOBzKda4IEhC8Lf.S2"
	if err := bcrypt.CompareHashAndPassword([]byte(seed), []byte("password")); err != nil {
		fmt.Println("seed hash does NOT match \"password\"")
	} else {
		fmt.Println("seed hash matches \"password\"")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("fresh hash: %s\n", hash)
}
