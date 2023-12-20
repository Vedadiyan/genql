package genql

import (
	"bytes"
	"fmt"
)

func DoubleQuotesToBackTick(str string) (string, error) {
	buffer := bytes.NewBufferString("")
	for i := 0; i < len(str); i++ {
		r := rune(str[i])
		switch r {
		case '\'':
			{
				buffer.WriteRune(r)
				i++
				r = '0'
				for ; i < len(str) && r != '\''; i++ {
					r = rune(str[i])
					buffer.WriteRune(r)
					if r == '\\' {
						if i+1 == len(str) {
							return "", fmt.Errorf("index out of range")
						}
						buffer.WriteRune(rune(str[i+1]))
						i++
					}
				}
				i--
			}
		case '`':
			{
				buffer.WriteRune(r)
				i++
				r = '0'
				for ; i < len(str) && r != '`'; i++ {
					r = rune(str[i])
					buffer.WriteRune(r)
				}
				i--
			}
		case '"':
			{
				buffer.WriteRune('`')
				i++
				r = '0'
				for ; i < len(str) && r != '"'; i++ {
					r = rune(str[i])
					if r == '"' {
						buffer.WriteRune('`')
						continue
					}
					if r == '\\' {
						if i+1 == len(str) {
							return "", fmt.Errorf("index out of range")
						}
						next := str[i+1]
						if next == '"' {
							buffer.WriteRune(rune(next))
							i++
							continue
						}
					}
					buffer.WriteRune(r)
				}
				i--
				continue
			}
		default:
			{
				buffer.WriteRune(r)
			}
		}
	}
	return buffer.String(), nil
}
