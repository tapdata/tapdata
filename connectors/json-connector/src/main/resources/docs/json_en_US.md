## **Connection Configuration Help**

### **1. Prerequisite description of file data source**
-Because of the particularity of file data source, connection configuration mainly includes file protocol specific configuration and file path. The connection failed to load the data model. The model can only be loaded after the required parameters corresponding to the file type have been configured in the task node. At present, one connection configuration only corresponds to one model.
-Incremental reading of file data source is through file wildcard scanning. Only new files or modifications of original files can be sensed. The scanning cycle is 1 minute by default. The data synchronization of deleting files and deleting file contents is not supported, and the involved files are added in full every time, and the purpose of modification is achieved by updating the condition field.

### **2. Supporting file protocol**
The following file protocol path delimiters use "/"
#### **LOCAL**
Local represents the file of the operating system where the local (engine) is located
#### **FTP**
FTP (File Transfer Protocol) can set the file server encoding.
#### **SFTP**
SFTP (Secure Encrypted File Transfer Protocol) can set the file server encoding. Linux system is enabled by default
#### **SMB**
SMB (File Sharing Protocol) Network file sharing protocol supported by Windows system, compatible with 1. x, 2. x, 3. x.
- Special note: When accessing a file share, select the shared directory first, and then fill in the path. (shared directory/file path)
#### **S3FS**
S3FS (file system following S3 protocol)

### **3. General parameters of task node**
#### **Model**
The name of the logical model built by the file selected by the task node
#### **Inclusion and exclusion wildcard (White&Black)**
Generic matching is only for * fuzzy matching and does not support regular expressions. Include Null indicates all files, and Exclude Null indicates no exclusion. The scanning file logic is to filter out the matched files from the matched files. The recursive switch indicates whether to traverse subdirectories.

### **4. json file configuration and usage**
The json file data source supports very large files.
#### **json file encoding**
If there is Chinese in the json file, you need to pay attention to the encoding method of the file content. For example, UTF-8 is the default in Linux, and GBK is the default in Windows.
#### **json type**
- JSON Object
```
{
  "A1":{"id":1, "name":"Jarad"},
  "A2":{"id":2, "name":"James"}
}
```
The above json object will be converted to A model of three fields, __key, id, and name, __key is the primary key
- JSON Array
```
[
  {"id":1, "name":"Jarad"},
  {"id":2, "name":"James"}
]
```

### **5. json file data type support**
- STRING
- TEXT
- INTEGER
- NUMBER
- BOOLEAN
- DATETIME
- OBJECT
- ARRAY