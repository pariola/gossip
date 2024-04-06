package kafka

import (
	"sync"
)

type logEntry struct {
	Offset  int64
	Message int64
}

type log struct {
	sync.RWMutex

	offset, commitedOffset int64
	entries                []logEntry
}

func newLog() *log {
	return &log{
		entries: make([]logEntry, 0),
	}
}

func (l *log) writeMessage(v int64) int64 {
	entry := logEntry{
		Offset:  l.offset,
		Message: v,
	}

	l.Lock()
	l.offset++
	l.entries = append(l.entries, entry)
	l.Unlock()

	return entry.Offset
}

func (l *log) readMessages(offset, atMost int64) [][2]int64 {
	l.RLock()
	defer l.RUnlock()

	if offset >= l.offset {
		return nil
	}

	lb, mid, rb := 0, 0, len(l.entries)-1

	for lb <= rb {
		mid = (lb + rb) / 2
		midOffset := l.entries[mid].Offset

		if offset == midOffset {
			break
		} else if offset > midOffset {
			lb = mid + 1
		} else {
			rb = mid - 1
		}
	}

	bound := len(l.entries) - 1
	if v := mid + int(atMost); v < bound {
		bound = v
	}

	var msgs [][2]int64

	for i := mid; i <= bound; i++ {
		entry := l.entries[i]
		msgs = append(msgs, [2]int64{entry.Offset, entry.Message})
	}

	return msgs
}

func (l *log) setCommitOffset(offset int64) {
	l.Lock()
	defer l.Unlock()
	l.commitedOffset = offset
}

func (l *log) getCommitOffset() int64 {
	l.RLock()
	defer l.RUnlock()
	return l.commitedOffset
}
