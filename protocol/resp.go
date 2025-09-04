package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Parser struct {
	reader *bufio.Reader
}

func NewParser(r io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(r),
	}
}

func (p *Parser) Parse() ([]string, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return nil, fmt.Errorf("empty line")
	}

	switch line[0] {
	case '*':
		return p.parseArray(line)
	default:
		return nil, fmt.Errorf("unsupported RESP type: %c", line[0])
	}
}

func (p *Parser) parseArray(line string) ([]string, error) {
	countStr := line[1:]
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %s", countStr)
	}

	if count < 0 {
		return nil, nil
	}

	args := make([]string, count)
	for i := 0; i < count; i++ {
		line, err := p.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("expected bulk string, got: %s", line)
		}

		lengthStr := line[1:]
		length, err := strconv.Atoi(lengthStr)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %s", lengthStr)
		}

		if length < 0 {
			args[i] = ""
			continue
		}

		data := make([]byte, length)
		_, err = io.ReadFull(p.reader, data)
		if err != nil {
			return nil, err
		}

		p.reader.ReadString('\n')

		args[i] = string(data)
	}

	return args, nil
}

func EncodeSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func EncodeBulkString(s string) string {
	if s == "" {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func EncodeInteger(i int) string {
	return fmt.Sprintf(":%d\r\n", i)
}

func EncodeError(msg string) string {
	return fmt.Sprintf("-ERR %s\r\n", msg)
}
