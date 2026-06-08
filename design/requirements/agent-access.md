# Purpose
We have a Globus search index with elasticsearch-like query capabilities. We want to identify the best way for AI tools such as Claude Code to work with data in the search index, according to certain key requirements.

This is not an exhaustive document and will be evolved by conversation with the AI tool.

Once the approach is identified, we will firm up requirements to make the approach secure, scalable, performant, and easy to use/deploy.

# Prior art
See `search_demo/agent-skill/` for an agent skill version of this tool, which contains a query script and instructions on how to chain tools. This provides key information on existing use cases.

It is ok to evaluate the existing tool against current requirements. There is no final answer yet.

# Key requirements
### The search process
* Dynamically select from 5-10 distinct search indices to find the one with the best data for this query
* Expose a schema of fields available in the search index to help the AI agent understand what data is available
* The LLM should be able to construct a query payload using documented globus search syntax (similar to elasticsearch)
* Minimize token usage by allowing the user to request only a subset of fields from the response. (suggest other strategies if appropriate)
  * Ensure that query steps happen in a deterministic way, eg, that post-processing always happens. Reduce risk that the AI would "forget"

### The user experience
* User defines a scientific goal in a conversational manner with the LLM. ("show me trending literature topics, and relevant protein structures for this molecule")
  * One or multiple queries may be initiated, and the results of one query may be used to build the next query
* Minimal setup required for the user. It is ok to assume they have a tool like Claude desktop, but avoid assumptions about whether they have a full featured development environment running locally (python, uv, docker, etc)
  * If installation steps are required, consider ways for the AI interface to be hosted on a remote server
  * Also make it easy to start an MCP running locally.

### Functionality
* Identify any security concerns for accepting user inputs. (eg avoid constructing CLI tool calls that would lead to prompt injection)
* Ok to assume that all data is public (no authentication required)

### System requirements
* Will be implemented in python. 
* The backend should use async strategies wherever possible. This is because the MCP server is talking to an external server.
  * Note: we will use the Globus SDK v4, but this does not support async requests to an external service.
* Preserve operational logs sufficient to detect abuse, such as by a rogue AI agent that consumes too many resources


# Design plan based on LLM discussions
* Use an MCP server with the following:
  * Server level instructions / data
    * How to construct a globus search query
  * Tools
    * `list_indices()` -> return `{index_id: description}`
    * `search(index_id, payload, fields_to_return=None)`  -> return search results
    * `get_schema(index_id)` -> return search index field schema. Use this to understand the data available in a search index in more detail.
