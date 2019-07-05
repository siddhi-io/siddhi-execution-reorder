Siddhi Execution Reorder
======================================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-reorder/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-reorder/)
  [![GitHub (pre-)Release](https://img.shields.io/github/release/siddhi-io/siddhi-execution-reorder/all.svg)](https://github.com/siddhi-io/siddhi-execution-reorder/releases)
  [![GitHub (Pre-)Release Date](https://img.shields.io/github/release-date-pre/siddhi-io/siddhi-execution-reorder.svg)](https://github.com/siddhi-io/siddhi-execution-reorder/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-execution-reorder.svg)](https://github.com/siddhi-io/siddhi-execution-reorder/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-execution-reorder.svg)](https://github.com/siddhi-io/siddhi-execution-reorder/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-execution-reorder extension** is a <a target="_blank" href="https://siddhi.io/">Siddhi</a> extension that orders out-of-order event arrivals using algorithms such as K-Slack and alpha K-Stack.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 5.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.execution.reorder/siddhi-execution-reorder/">here</a>.
* Versions 4.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.execution.reorder/siddhi-execution-reorder">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-execution-reorder/api/5.0.3">5.0.3</a>.

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-reorder/api/5.0.1/#akslack-stream-processor">akslack</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">(Stream Processor)</a>*<br><div style="padding-left: 1em;"><p>This stream processor extension performs reordering of an event stream which is out of order.<br>&nbsp;It implements the AQ-K-Slack based out-of-order handling algorithm which is originally described in <br>'http://dl.acm.org/citation.cfm?doid=2675743.2771828'.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-reorder/api/5.0.1/#kslack-stream-processor">kslack</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#stream-processor">(Stream Processor)</a>*<br><div style="padding-left: 1em;"><p>This stream processor extension performs reordering of an out-of-order event stream.<br>&nbsp;It implements the K-Slack based out-of-order handling algorithm which is originally described in <br>'https://www2.informatik.uni-erlangen.de/publication/download/IPDPS2013.pdf'.)</p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-reorder/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.
