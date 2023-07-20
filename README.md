# beakers

beakers is an experimental lightweight declarative ETL framework for Python

Right now this is an experiment to explore some ideas around ETL.

It is still very experimental with no stability guarantees. 
If you're interested in poking around, thoughts and feedback are welcome, please reach out before contributing code though as a lot is still in flux.

## (Intended) Features

- [x] Declarative ETL graph comprised of Python functions & Pydantic models
- [x] Developer-friendly CLI for running processes
- [x] Synchronous mode for ease of debugging or simple pipelines
- [x] Data checkpoints stored in local database for intermediate caching & resuming interrupted runs
- [ ] Asynchronous task execution
- [ ] Support for multiple backends (sqlite, postgres, etc)
- [ ] Robust error handling, including retries

## Guiding Principles

* **Lightweight** - Writing a single python file should be enough to get started. It should be as easy to use as a script in that sense.
* **Data-centric** - Looking at the definition should make it clear what data exists at what step. 
* **Modern Python** - Take full advantage of recent additions to Python, including type hints, `asyncio`, and libraries like `pydantic`.
* **Developer Experience** - The focus should be on the developer experience, a nice CLI, helpful error messages.

## Anti-Principles

Unlike most tools in this space, this is not a complete "enterprise grade" ETL solution.

It isn't a perfect analogy by any means but beakers strives to be to `luigi` what `flask` is to `Django`. 
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