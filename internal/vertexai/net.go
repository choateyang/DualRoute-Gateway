package vertexai

import "dualroute-gateway/internal/transport"

func (c *VertexAIClient) Net() *transport.NetworkClient { return c.net }
