package thrift

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sync"
)

var Debug bool

type TClient interface {
	Call(ctx context.Context, method string, args, result TStruct) error
}

type TStandardClient struct {
	seqId int32

	mutex        sync.Mutex
	iprot, oprot TProtocol
}

// TStandardClient implements TClient, and uses the standard message format for Thrift.
// It is not safe for concurrent use.
func NewTStandardClient(inputProtocol, outputProtocol TProtocol) *TStandardClient {
	return &TStandardClient{
		iprot: inputProtocol,
		oprot: outputProtocol,
	}
}

func (p *TStandardClient) Send(ctx context.Context, oprot TProtocol, seqId int32, method string, args TStruct) error {
	// Set headers from context object on THeaderProtocol
	if headerProt, ok := oprot.(*THeaderProtocol); ok {
		headerProt.ClearWriteHeaders()
		for _, key := range GetWriteHeaderList(ctx) {
			if value, ok := GetHeader(ctx, key); ok {
				headerProt.SetWriteHeader(key, value)
			}
		}
	}

	if err := oprot.WriteMessageBegin(method, CALL, seqId); err != nil {
		return err
	}
	if err := args.Write(oprot); err != nil {
		return err
	}
	if err := oprot.WriteMessageEnd(); err != nil {
		return err
	}
	return oprot.Flush(ctx)
}

func (p *TStandardClient) Recv(iprot TProtocol, seqId int32, method string, result TStruct) error {
	rMethod, rTypeId, rSeqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return err
	}

	if method != rMethod {
		return NewTApplicationException(WRONG_METHOD_NAME, fmt.Sprintf("%s: wrong method name, %s", method, rMethod))
	} else if seqId != rSeqId {
		return NewTApplicationException(BAD_SEQUENCE_ID, fmt.Sprintf("%s: out of order sequence response", method))
	} else if rTypeId == EXCEPTION {
		var exception tApplicationException
		if err := exception.Read(iprot); err != nil {
			return err
		}

		if err := iprot.ReadMessageEnd(); err != nil {
			return err
		}

		return &exception
	} else if rTypeId != REPLY {
		return NewTApplicationException(INVALID_MESSAGE_TYPE_EXCEPTION, fmt.Sprintf("%s: invalid message type", method))
	}

	if err := result.Read(iprot); err != nil {
		return err
	}

	return iprot.ReadMessageEnd()
}

func (p *TStandardClient) Call(ctx context.Context, method string, args, result TStruct) error {
	p.mutex.Lock()
	p.seqId++
	seqId := p.seqId
	p.mutex.Unlock()

	var req string
	var res string
	defer func() {
		if Debug {
			fmt.Println(fmt.Sprintf("%s--[%s] req: %s, res: %s ",
				time.Now().Format("2006-01-02 15:04:05"), method, req, res))
		}
	}()

	if Debug {
		_req, err := json.Marshal(args)
		if nil == err {
			req = string(_req)
		} else {
			req = string("Not Request Body.")
		}
	}

	if err := p.Send(ctx, p.oprot, seqId, method, args); err != nil {
		return err
	}

	// method is oneway
	if result == nil {
		return nil
	}

	err := p.Recv(p.iprot, seqId, method, result)
	if Debug && nil == err {
		_res, err := json.Marshal(result)
		if nil == err {
			res = string(_res)
		} else {
			res = string("Not Response body.")
		}
	}

	return err
}
