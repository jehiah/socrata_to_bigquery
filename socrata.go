package main

import "fmt"

func LogSocrataSchema(c []SocrataColumn) {
	for i, cc := range c {
		fmt.Printf("[%d] %q (%s) %s\n", i, cc.FieldName, cc.DataTypeName, cc.Name)
	}
}
