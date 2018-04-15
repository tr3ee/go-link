package link

import (
	"context"
	"io"
)

const (
	defaultBufferSize = 1024
)

type callbackFunc func([]byte) []byte

// OneWayLink is the shortcut to OneWayLinkSpec without buffer
func OneWayLink(ctx context.Context, src io.Reader, dst io.Writer, cb callbackFunc) (int64, error) {
	return OneWayLinkSpec(ctx, src, dst, nil, cb)
}

// OneWayLinkSpec maintains a one-way link from src to dst
// until either EOF is reached on src or an error occurs.
// It returns the number of bytes transferred and the first
// error encountered, if any.
func OneWayLinkSpec(ctx context.Context, src io.Reader, dst io.Writer, buf []byte, cb callbackFunc) (written int64, err error) {
	if buf == nil {
		buf = make([]byte, defaultBufferSize)
	}
	for {
		rn, er := src.Read(buf)
		if ctx != nil {
			select {
			case <-ctx.Done():
				break
			default:
			}
		}
		if rn > 0 {
			tbuf := buf[:rn]
			if cb != nil {
				tbuf = cb(tbuf)
			}
			wn, ew := dst.Write(tbuf)
			if wn > 0 {
				written += int64(wn)
			}
			if ew != nil {
				err = ew
				break
			}
			if wn != rn {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return
}

// TwoWayLink is the shortcut to TwoWayLinkSpec without buffer
func TwoWayLink(ctx context.Context, h1, h2 io.ReadWriteCloser, cb1, cb2 callbackFunc) (w1, w2 int64, e1, e2 error) {
	return TwoWayLinkSpec(ctx, h1, h2, nil, nil, cb1, cb2)
}

// TwoWayLinkSpec maintains a two-way link between X and Y.
// The end of one link will shut down the entire link. It
// returns the number of bytes that h1 and h2 write to each
// other and the first error encountered, if any.
func TwoWayLinkSpec(ctx context.Context, h1, h2 io.ReadWriteCloser, buf1, buf2 []byte, cb1, cb2 callbackFunc) (w1, w2 int64, e1, e2 error) {
	var err error
	exit := make(chan struct{}, 0)
	go func() {
		w1, err = OneWayLinkSpec(ctx, h1, h2, buf1, cb1)
		if err != nil {
			e1 = err
		}
		h1.Close()
		h2.Close()
		close(exit)
	}()
	w2, err = OneWayLinkSpec(ctx, h2, h1, buf2, cb2)
	if err != nil {
		e2 = err
	}
	h2.Close()
	h1.Close()
	<-exit
	return
}
