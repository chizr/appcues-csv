# AppCues CSV Parser
Turns out AppCues CSV files can be *big*, and cause Numbers<sup>1</sup> to have a panic attack.
Using the magic of Node Streams, we can process the file line-by-line for reduced RAM use/user sadness.

- Install the deps using your favourite package manager
- Run the program with `./index.js --infile=appcues-export.csv --outfile=stuffyouwant.txt [--maxlen N]`

The `maxlen` argument is optional, and allows output to be split over mutiple files,
e.g. for if you want to run a database query (_technically_ the `outfile` arg is also optional, but your filtered data gets sent to a no-op stream. Poor data.)

<sup>1</sup>Or other spreadsheet program of your choice, I guess.
