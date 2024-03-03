# ProvETL Instrument Talend Job

Instrument Talend Job is a complementary script to the [ProvETL tool](https://github.com/mathunes/prov-etl) used to automatically add the provenance data collection scripts to the data processing routines defined in the Talend tool.

## Installation

To run this script, you need to have the ProvETL server running. The steps for installing ProvETL are available in the [tool's repository](https://github.com/mathunes/prov-etl).

1. Update the `URL_API` variable in the script with the current URL of the running ProvETL API
2. Install the `requests` library with the following command

```
pip install requests
```

## Usage

To instrument a Talend Job, you need to obtain the routine's configuration XML. This is usually available in the path: 

`\<workspace name>\<project name>\process\<job name>.ITEM`

Once you have this path, just run the following command to perform the instrumentation.

`python3 instrument_job.py <path of job XML>`

At the end of execution, if all goes well, the job will contain the scripts for communicating with ProvETL.

In general, this instrumentation consists of collecting the prospective provenance of a routine, i.e. the structure and execution sequence of the defined components. This first version only collects provenance data from FLOW connections. In addition, the added scripts are also ready to make calls to the ProvETL tool at runtime, and thus collect retrospective provenance. The collected data can be accessed in the web interface after execution.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT License

Copyright (c) 2024 ProvETL

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
