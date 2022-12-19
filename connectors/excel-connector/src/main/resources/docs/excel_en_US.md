## **Connection Configuration Help**

### **1. Prerequisite description of file data source**
- Because of the particularity of file data source, connection configuration mainly includes file protocol specific configuration and file path. The connection failed to load the data model. The model can only be loaded after the required parameters corresponding to the file type have been configured in the task node. At present, one connection configuration only corresponds to one model.
- Incremental reading of file data source is through file wildcard scanning. Only new files or modifications of original files can be sensed. The scanning cycle is 1 minute by default. The data synchronization of deleting files and deleting file contents is not supported, and the involved files are added in full every time, and the purpose of modification is achieved by updating the condition field.

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

### **4. Excel file configuration and usage**
Excel file parsing in xls and xlsx formats is supported. It can be adapted to forms with merged cells and forms with formula input, but super large files are not supported temporarily.
#### **Document password**
If the Excel file is set with a password, it can be decrypted through this setting
#### **Page range**
If it is empty, all sheet pages are loaded by default. Format example: 1,3-5,8 represents page 1, 3, 4, 5 and 8.
#### **Data column range**
Format example: B~BA represents columns B to BA
#### **Excel header and data row**
Excel files can be configured with a row as the header, or you can specify the header (comma separated). When the header line is 0, there is no header line in the CSV file. If the header is empty at the same time, it will be automatically named as Column1, Column2.

### **5. Excel file data type support**
- STRING
- TEXT
- DOUBLE
- BOOLEAN
- DATE