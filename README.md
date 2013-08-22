# Storm Test #

## Overview ##
This test comprises of a Storm topology. More information on Storm can be found [here](http://storm-project.net/ "here").

The topology reads the contents of a local file, splitting the text into single lines and then into individual words.  For each word in the file a line is written to an output file stating the word and the number of times that word has been previously seen.

## Tasks ##

1. Perform a code review of the application describing the changes you would suggest to the developer. 
2. Add a Bolt (or series of Bolts) to the Topology so that in addition to the current functionality, for each letter in the file, a line is written to a new file stating the letter and the number of times that letter has previously been seen.
3. Describe some improvements/new features which could be made to extend this application.
4. Implement some of the improvements you listed in task 3.

## Notes ##

- We are not looking for a complete rewrite of the application for the code review. Just some helpful tips on best practices.
