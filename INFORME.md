# Decisión de diseño

Se decidió implementar BaseMiddleware al notar que las implementaciones de QueueMiddleware y ExchangeMiddleware tenían partes de código idénticas. Como en Go no hay clases y por ende no se puede hacer herencia, se investigó la manera de lograr algo similar en Go, y se llegó a **Composición**. De esta manera, BaseMiddleware contiene en su struct la información que necesitan tanto exchange como queue, así como las funciones StopConsuming, Close y la función auxiliar consumeMessages, también 3 funciones que ambas implementaciones utilizan.

De esta manera se logra que no se repita tanto código entre QueueMiddleware y ExchangeMiddleware.