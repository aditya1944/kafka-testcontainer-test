package main

import (
	"context"
	"fmt"
)

func handler(ctx context.Context, person *Person) error {
	fmt.Println(person.Name)
	return nil
}
