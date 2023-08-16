# beakers

beakers is an experimental lightweight declarative ETL framework for Python

It is still very much in flux with no correctness or stability guarantees. 

No contributions yet please, but feel free to poke around/ask questions.

## Features

- declarative ETL graph comprised of Python functions & Pydantic models
- developer-friendly CLI for running processes
- sync/async task execution
- data checkpoints stored in local database for intermediate caching & resuming interrupted runs
- robust error handling, including retries

## Guiding Principles

* **Lightweight** - Writing a single python file should be enough to get started. It should be as easy to use as a script in that sense.
* **Data-centric** - Know what data is added at each step.
* **Modern Python** - Take full advantage of recent additions to Python, including type hints, `asyncio`, and libraries like `pydantic` and `rich`.
* **Developer Experience** - Focused on the developer experience: a nice CLI, helpful error messages.

## Anti-Principles

Unlike most tools in this space, this is not a complete "enterprise grade" ETL solution.

It isn't a perfect analogy by any means but it could be said `databeakers` is to `luigi` what `flask` is to `Django`.
If you are building your entire business around ETL, it makes sense to invest in the infrastructure & tooling to make that work.
Maybe structuring your code around beakers will make it easier to migrate to one of those tools than if you had written a bespoke script.
Plus, beakers is Python, so you can always start by running it from within a bigger framework.

## Concepts

Like most ETL tools, beakers is built around a directed acyclic graph (DAG).

The nodes on this graph are known as "beakers", and the edges are often called "transforms".

(Note: These names aren't final, suggestions welcome.)

### Beakers

Each node in the graph is called a "beaker". A beaker is a container for some data.

Each beaker has a name and a type.
The name is used to refer to the beaker elsewhere in the graph.
The type, represented by a `pydantic` model, defines the structure of the data. By leveraging `pydantic` we get a lot of nice features for free, like validation and serialization.

### Transform

Edges in the graph represent dataflow between beakers. Each edge has a concept of a "source beaker" and a "destination beaker".

 These come in two main flavors:

* **Transforms** - A transform places new data in the destination beaker based on data already in the source beaker.
An example of this might be a transform that takes a list of URLs and downloads the HTML for each one, placing the results in a new beaker.

* **Filter** - A filter can be used to stop the flow of data from one beaker to another based on some criteria.

### Seed

A concept somewhat unique to beakers is the "seed". A seed is a function that returns initial data for a beaker.

This is useful for things like starting the graph with a list of URLs to scrape, or a list of images to process.

A beaker can have any number of seeds, for example one might have a short list of URLs to use for testing, and another that reads from a database.