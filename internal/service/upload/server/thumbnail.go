package upload

import (
	"bytes"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"strconv"
)

// Thumbnail renders a max-edge JPEG preview for image objects.
func (s *Service) Thumbnail(key string, maxEdge int) ([]byte, error) {
	cacheKey := key + ":" + strconv.Itoa(maxEdge)
	s.thumbMu.Lock()
	if cached, ok := s.thumbs[cacheKey]; ok {
		s.thumbMu.Unlock()
		return cached, nil
	}
	s.thumbMu.Unlock()
	object, _, _, err := s.Open(key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = object.Close() }()
	source, _, err := image.Decode(object)
	if err != nil {
		return nil, err
	}
	bounds := source.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if maxEdge <= 0 {
		maxEdge = 320
	}
	if width > maxEdge || height > maxEdge {
		scale := float64(maxEdge) / float64(width)
		if height > width {
			scale = float64(maxEdge) / float64(height)
		}
		width = int(float64(width) * scale)
		height = int(float64(height) * scale)
	}
	target := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			sx := bounds.Min.X + x*bounds.Dx()/width
			sy := bounds.Min.Y + y*bounds.Dy()/height
			target.Set(x, y, source.At(sx, sy))
		}
	}
	var buffer bytes.Buffer
	if err = jpeg.Encode(&buffer, target, &jpeg.Options{Quality: 80}); err != nil {
		return nil, err
	}
	data := buffer.Bytes()
	s.thumbMu.Lock()
	if len(s.thumbs) >= 1024 {
		s.thumbs = make(map[string][]byte)
	}
	s.thumbs[cacheKey] = data
	s.thumbMu.Unlock()
	return data, nil
}
