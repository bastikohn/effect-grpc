# AGENTS.md

## General Instructions

Must-comply with for all tasks: **Always try to reduce complexity and reduce the amount of code in this library.**

## Project status

This library is brand new - no active users. It's ok to introduce breaking changes, if they improve the code quality, ergonomics, or performance.

## `opensrc`

- Use `opensrc` for raw source access to this project's `effect` and `grpc` libraries (this branch targets Effect v3; `main` targets v4).
- Feel free to search in `~/.opensrc/` directly. The repos may already exist. Treat this directory as a read-only reference source.
- `opensrc` is not available in the host's `$PATH`, try using the project's flake or raw Nix commands to access/run it.
