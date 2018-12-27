# cdr-reusable-code

This is the CDR main code. This repository contains:
<ol>
  <li>Extract important columns which we usually need are <b>datetime of call/text</b>, <b>type of interaction (call/text)</b>, <b>direction call/text</b> (incoming call, outgoing call, incoming text and outgoing call),  <b>duration time of call</b>, <b>caller_id</b> or <b>called_id</b>, <b>antenna</b> which been used. Filename: <i>cdr-extraction.py</i></li>
  <li>Transform important columns extracted for each user. Filename: <i>user-extraction.py</i></li>
  <li>Generate bandicoot indicators per week. Details for bandicoot module in <a href='http://bandicoot.mit.edu/' target='_blank'>here</a>. Filename: <i>generate-bandicoot.py</i></li>
  <li>Compiled all bandicoot indicators as one file result per week. Filename: <i>bandicoot-concat.py</i></li>
</ol> 
