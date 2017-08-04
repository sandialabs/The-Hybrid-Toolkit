## Synopsis

The goal of this directory is to contain the **example tutorial 1** for Sandia national labs hybrid toolkit. This will be the most simple example that shows the hybrid pipe line

####This is an example of how to use hybrid in a "streaming" environment 

***

## Runnning The Code Example

Run the following from the example_1 dir

```bash
python database_data_generator.py
```

in a seperate terminal run the following from the example_1 dir

```bash
python hybrid_executor.py --cfg=example.json
```
**Assumes**

- you have the hybrid toolkit in your python package list 
- that hybrid_executor.py has been added to /bin

***

## Motivation

There was a need for simple examples on how to use the hybrid pipeline

## Installation

Install hybrid into your python path and run the example code.

## API Reference

See Docs directory.

## Tests

No tests are planned for the examples.

## Contributors

- Matthew Letter mletter@sandia.gov
- Warren Davis

## License

Copyright (c) 2015 **Sandia National Labs**
