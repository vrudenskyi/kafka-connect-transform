
# kafka-connect-transform
Kafka Connect Transformations Enhancements

#### Key features:
* Can be applied to different parts of a message: key, value, headers, topic
* Support structured and text data
* If conditions supported 

#### List of tranfsormations 
| Transformation | Description
|-|-
| [AddHeader](../../wiki/AddHeader) | Add record header to message
| [RegexRules](../../wiki/RegexRules) | Apply regular expressions to  message
| [RegexRouter](../../wiki/RegexRouter) | Route message to a different topic based on regular expressions
| [RegexFilter](../../wiki/RegexFilter) | Filter messages with regular expressions
| [MapTranslate](../../wiki/MapTranslate) | 'Translate' one string value with another using translation map
| TimestampConverter | Parse/convert 'time' strings
| RemoveField | Remove field (for structured data)
| ExtractField | Extract field (for structured data) 
| ToJson | Transforms any jackson supported format to JSON
| ParseSyslog | Parse syslog message
| KVPairParser | Configurable Key-Value pair
| CEF2Map | CEF format parser

