# go-passthrough

A simple RabbitMQ message forwarder.

## What

This is a simple service that connects to a RabbitMQ broker, subscribes to the specified queues, and starts a process with the message body passed to stdin.

## Example

The following example will consume messages from a queue (`marketing.customer.created`). When a messages is published, go-passthrough will start a new process, invoke `handler.php` which in turn will dump the message to a file.

config.yml
```yml
rabbit:
  connectionString: amqp://vagrant:vagrant@192.168.50.2:5672/
  queues:
    - marketing.customer.created

command:
  proc: php
  args: -f handle.php
```

handler.php
```php
<?php
$data = file_get_contents("php://stdin");
file_put_contents(rand() . "test.txt", $data);
```
