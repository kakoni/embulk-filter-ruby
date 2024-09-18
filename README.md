# TruffleRuby filter for Embulk

Uses TruffleRuby to run filter functions

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
      # Add a new field 'greeting' based on 'name'
      record['greeting'] = "Hello, #{record['name']}!"
      # Modify the 'age' field
      record['a'] = record['a'] + "b"
      # Return the modified record
      record
   
 ```

## Build

```
$ ./gradlew gem
```


