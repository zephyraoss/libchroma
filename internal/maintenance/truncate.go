package maintenance

import (
	"fmt"
	"os"

	"github.com/zephyraoss/libchroma/internal/cktype"
	"github.com/zephyraoss/libchroma/internal/wire"
)

func TruncateOverflow(path string, baseSize int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening file for overflow truncate: %w", err)
	}
	defer f.Close()

	var magic [8]byte
	if _, err := f.ReadAt(magic[:], 0); err != nil {
		return fmt.Errorf("reading header magic: %w", err)
	}
	var footerMagic [8]byte
	switch magic {
	case wire.MagicCKD:
		footerMagic = wire.FooterMagicCKD
	case wire.MagicCKX:
		footerMagic = wire.FooterMagicCKX
	case wire.MagicCKM:
		footerMagic = wire.FooterMagicCKM
	case wire.MagicCKI:
		footerMagic = wire.FooterMagicCKI
	default:
		return fmt.Errorf("%w: %q", cktype.ErrBadMagic, magic[:])
	}

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if baseSize < wire.HeaderSize+wire.FooterSize || baseSize > fi.Size() {
		return fmt.Errorf("ckaf: overflow truncate size %d out of range for %d-byte file", baseSize, fi.Size())
	}

	if err := f.Truncate(baseSize); err != nil {
		return err
	}
	if err := wire.WriteFooter(f, baseSize-wire.FooterSize, cktype.Footer{Magic: footerMagic}); err != nil {
		return err
	}
	if err := wire.ClearHeaderFlagBit(f, f, 0x2); err != nil {
		return err
	}
	return f.Sync()
}
