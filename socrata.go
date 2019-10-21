package main

import (
	"fmt"

	soda "github.com/SebastiaanKlippert/go-soda"
)

func LogSocrataSchema(c []soda.Column) {
	for i, cc := range c {
		fmt.Printf("[%d] %q (%s) %s %#v\n", i, cc.FieldName, cc.DataTypeName, cc.Name, cc)
	}
}

