package queue

import (
	"encoding/json"
	"fmt"
	"net/url"

	protobufv1 "github.com/internetarchive/Zeno/internal/pkg/queue/protobuf/v1"
	"google.golang.org/protobuf/proto"
)

func encodeItem(item *Item) ([]byte, error) {
	urlJSON, err := json.Marshal(item.URL)
	if err != nil {
		return nil, err
	}

	parentURLJSON, err := json.Marshal(item.ParentURL)
	if err != nil {
		return nil, err
	}

	protoItem := &protobufv1.ProtoItem{
		Url:             urlJSON,
		ParentUrl:       parentURLJSON,
		ID:              item.ID,
		Hop:             item.Hop,
		Type:            item.Type,
		BypassSeencheck: item.BypassSeencheck,
		Hash:            item.Hash,
		Redirect:        item.Redirect,
		LocallyCrawled:  item.LocallyCrawled,
	}

	return proto.Marshal(protoItem)
}

func decodeProtoItem(itemBytes []byte) (*Item, error) {
	protoItem := &protobufv1.ProtoItem{}
	err := proto.Unmarshal(itemBytes, protoItem)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal item: %w", err)
	}

	var URL url.URL
	err = json.Unmarshal(protoItem.Url, &URL)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal URL: %w", err)
	}

	var parentURL url.URL
	err = json.Unmarshal(protoItem.ParentUrl, &parentURL)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal URL: %w", err)
	}

	return &Item{
		URL:             &URL,
		ParentURL:       &parentURL,
		Hop:             protoItem.GetHop(),
		Type:            protoItem.GetType(),
		ID:              protoItem.GetID(),
		BypassSeencheck: protoItem.GetBypassSeencheck(),
		Hash:            protoItem.GetHash(),
		Redirect:        protoItem.GetRedirect(),
		LocallyCrawled:  protoItem.GetLocallyCrawled(),
	}, nil
}
