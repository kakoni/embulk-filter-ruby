# TruffleRuby filter for Embulk

Uses TruffleRuby to run filter functions. Use 
JDK from https://github.com/graalvm/graalvm-ce-builds/releases to run this. This build assumes version 23.

## Overview

* **Plugin type**: filter
* **Guess supported**: no
* Embulk 0.11 or later


## Example

```yaml
in:
  type: any file input plugin type
  ...
filters:
  - type: ruby
    ruby_code: |
      puts "Hello from ruby"

      # Modify the 'a' field
      record['a'] = record['a'] + "b"
      # Return the modified record
      record
   
 ```

## Build

```
$ ./gradlew gem
```


