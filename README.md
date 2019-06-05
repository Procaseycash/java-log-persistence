## Parser Library
This is used for reading log files and processing IP address request period for a given time interval**

### Author

*Kazeem Olanipekun*

### Build (.jar)
The build version of the application can be located at `/build` from the root of the app.

### Sample Test Using the `access.log` 

![Alt text](img/sample.png?raw=true "ng-pure-datatable")

### Command Used

` java -cp "parser.jar" com.ef.Parser --accesslog=/Users/k.olanipekun/Documents/Applications/Parser/log/access.log --startDate=2017-01-01.13:00:00 --duration=hourly --threshold=100
`

Thank you