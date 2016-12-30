# NiFi GetTCP Processor 

Uses Netty4. 

Allows you to specify the following properties

- Server Address
- Reconnect delay
- TCP Read Timeout
- Message Delimiter

Support reconnection strategy based on read timeouts and connection timeouts.

Allows you to specify a message delimiter so that flowfiles are only generated after the delimiter was found in the TCP stream.  

Each flowfile that is produced by this processor will have the following attributes 

- tcp.sender
- tcp.port