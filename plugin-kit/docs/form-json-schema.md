# Config Options

Describe a form for user to input the information for how to connect and which database to open and any other information need user to fill. 

To describe the UI components for user to input, Use "${variable}" stand for variable that pdk developer should replace it actually values.   
- String
```json
{
  "type": "object",
    "properties": {
      "${inputKey}": {
        "type": "string",
        "title": "${inputTitle}",
        "x-decorator": "FormItem",
        "x-component": "Input"
      }
    }
}
```
- Password  
```json
{
  "type": "object",
    "properties": {
      "${inputKey}": {
        "type": "string",
        "title": "${inputTitle}",
        "x-decorator": "FormItem",
        "x-component": "Password"
      }
    }
}
```
- Number
```json
{
  "type": "object",
    "properties": {
      "${inputKey}": {
        "type": "number",
        "title": "${inputTitle}",
        "x-decorator": "FormItem",
        "x-component": "Password"
      }
    }
}
```

Common attributes are below, 
```json
{
  "type": "object",
    "properties": {
      "${inputKey}": {
        ...
        "default": "${defaultValue}", //Default value
        "required": true, //User must input or not
        ...
      }
    }
}
```

For example, for a connector, need user input below information, 
- Host
- Port
- Database

Then the configOptions will be, 
```json
{
  "configOptions": {
    "connection": {
      "type": "object",
      "properties": {
        "host": {
          "type": "string",
          "title": "Host",
          "required": true,
          "x-decorator": "FormItem",
          "x-component": "Input"
        },
        "port": {
          "type": "number",
          "title": "Port",
          "required": true,
          "x-decorator": "FormItem",
          "x-component": "Input"
        },
        "database": {
          "type": "string",
          "title": "Database",
          "required": true,
          "x-decorator": "FormItem",
          "x-component": "Input"
        }
      }
    }
  }
}
```

