# Arak

A general purpose Ethereum event indexing service. Ethereum logs are decoded
into Solidity events and stored in an SQL database, allowing for powerful
relational queries.

## Name

Arak (/əˈɹæk/) is a distilled anis spirit, and completely unrelated to this
project. The name was chosen as this project is meant as a light-weight
self-hosted alternative to Dune. Dune led to Arrakis, which led to Arak (to
avoid any potential trademark issues).

## Running

In order to execute the project locally:

```sh
cp arak.example.toml arak.toml
${EDITOR} arak.toml # fill in stuff
cargo run
```
