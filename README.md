# ocilogs-for-fluent-bit

Fluent Bit Plugin for Oracle Cloud Logs

## Usage

ocilogs.conf
```
[SERVICE]
  flush 5
  plugins_file ./plugins.conf

[INPUT]
  name winstat
  tag  event-log

[OUTPUT]
  name   ocilogs
  match  event*
  source Windows
  subject Testing
  log_id <YOUR LOG ID>
```

plugins.conf
```
[PLUGINS]
    Path c:/dev/tools/fluent-bit/bin/ocilogs.dll

```

## Requirements

* fluent-bit

## Installation

UNIX

```
$ make
```

Windows

```
$ make windows-release
```

## License

MIT

## Author

Yasuhiro Matsumoto (a.k.a. mattn)
