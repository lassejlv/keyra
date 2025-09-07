package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Parser struct {
	reader     *bufio.Reader
	bufferPool *BufferPool
	stringsPool *StringsPool
}

func NewParser(r io.Reader) *Parser {
	return &Parser{
		reader:      bufio.NewReaderSize(r, 32768),
		bufferPool:  DefaultBufferPool,
		stringsPool: DefaultStringsPool,
	}
}

func NewParserWithPools(r io.Reader, bufferPool *BufferPool, stringsPool *StringsPool) *Parser {
	return &Parser{
		reader:      bufio.NewReaderSize(r, 32768),
		bufferPool:  bufferPool,
		stringsPool: stringsPool,
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

	if count > 1000000 {
		return nil, fmt.Errorf("array too large: %d elements", count)
	}

	args := p.stringsPool.Get()
	if cap(args) < count {
		args = make([]string, count)
	} else {
		args = args[:count]
	}

	buf := p.bufferPool.Get()
	defer p.bufferPool.Put(buf)

	for i := 0; i < count; i++ {
		line, err := p.reader.ReadString('\n')
		if err != nil {
			p.stringsPool.Put(args)
			return nil, err
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 || line[0] != '$' {
			p.stringsPool.Put(args)
			return nil, fmt.Errorf("expected bulk string, got: %s", line)
		}

		lengthStr := line[1:]
		length, err := strconv.Atoi(lengthStr)
		if err != nil {
			p.stringsPool.Put(args)
			return nil, fmt.Errorf("invalid bulk string length: %s", lengthStr)
		}

		if length < 0 {
			args[i] = ""
			continue
		}

		if length > 512*1024*1024 {
			p.stringsPool.Put(args)
			return nil, fmt.Errorf("bulk string too large: %d bytes", length)
		}

		if cap(buf) < length {
			buf = make([]byte, length)
		} else {
			buf = buf[:length]
		}

		_, err = io.ReadFull(p.reader, buf)
		if err != nil {
			p.stringsPool.Put(args)
			return nil, err
		}

		p.reader.ReadString('\n')

		args[i] = string(buf)
	}

	return args, nil
}

func (p *Parser) ReleaseArgs(args []string) {
	if args != nil {
		p.stringsPool.Put(args)
	}
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
