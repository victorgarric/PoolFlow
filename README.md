# PoolFlow

<center>

**A simple process management library**

</center>

## Introduction

**PoolFlow** provides simple objects to manage the execution of a variety of tasks and 
its memory usage. The main objective of **PoolFlow** is to perform a lot of scientific calculations
 and manage their successive executions through the subprocess library.

## Schematic principle

The way **PoolFlow** works could be synthesized by the following diagram. A list of jobs is 
given and the `pool` object manage the available virtual memory, considering the maximum estimated
cost of the job given by the user.

<center>

``` mermaid
graph LR
A[Job with<br>given cost]-->|Set of parameters|B{Pool};
B-->C[Estimated usage<br>memory];
C-->|Not enough<br>memory|B;
C-->|Memory available<br>and allocated|D[Job is<br>launched];
D-->|Job is done<br>memory est released|B;

```
</center>

## Installing 
The only external package **SourceFlow** depends on is `rich`. Install it directly via `pip`:

<center>

`pip install PoolFlow`

</center>

## [Documentation](https://victorgarric.github.io/PoolFlow/)


## Warning and known issues

**PoolFlow** uses management resources via `resource` native package. Due to the way `resource` work, some limitation
and unlimitation procedures are only available on Unix.

