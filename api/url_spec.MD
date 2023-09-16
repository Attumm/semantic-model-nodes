# URL Specification

## Introduction

The API provides a mechanism to build complex query strings using different nodes and their fields. This document outlines the URL specification for constructing such queries.

## Base URL

The base URL is given by `<host>`.

## 1. Retrieving Available Nodes and Their Fields

### 1.1. Endpoint

```
<host>/api/list-nodes
```

### 1.2. Parameters

- `format`: Expected output format. Example value: `json`
- `groupby`: The parameter to group the result by. Example value: `node`

### 1.3. Example

```
<host>/api/list-nodes?format=json&groupby=node
```

## 2. Retrieving Filter Options

### 2.1. Endpoint

```
<host>/api/sm-query-options/
```

## 3. Retrieving Link Possibilities for a Given Node and Field

### 3.1. Endpoint

```
<host>/api/link-possible/<dn>/<field>
```

Where:
- `<dn>`: The main node.
- `<field>`: Field of the main node.

## 4. Building the Query String

### 4.1. Key Components

- `dn`: Main node.
- `fields`: Fields of nodes.
- `filter`: Filters to apply. Multiple filters are allowed, and their order matters.
- `link`: Links between fields of different nodes. Multiple links are allowed, and their order matters.
- `orderby`: Fields by which to order the results. Multiple order-by fields are allowed, and their order matters.

### 4.2. Query String Construction

The general format is:

```
dn=<encoded dn>&field=<encoded field1>&field=<encoded field2>&...&filter=<encoded filter>&...&link=<encoded link>&...&orderby=<encoded orderby>&...
```

Where:

- `<encoded dn>`: URL encoded name of the active node.
- `<encoded field>`: URL encoded field in the format `parent.field_name`.
- `<encoded filter>`: URL encoded filter in the format `operator:parent.field_name:operator_input`.
- `<encoded link>`: URL encoded link in the format `parent.field_name:operator.operator_input`.
- `<encoded orderby>`: URL encoded orderby in the format `operator:parent.field_name`.

### 4.3. Key Notes:

1. All filters, fields, links, and orderby parameters can occur multiple times and in any order.
2. The order of the filter, link, and orderby parameters in the query string matters and determines the order of their application.
3. When multiple fields, filters, links, or orderby parameters exist, they are separated using `&`.

## 5. Example:

Suppose you have:
- `dn` as `domain.address`
- Two fields: `domain.address.standard_id` and `domain.address.value`
- A filter: `match:domain.address.standard_id:input1`
- A link: `domain.address.standard_id:eq.domain.arp.standard_id`

The constructed query string will be:

```
<host>/api/gen?dn=domain.address&field=domain.address.standard_id&field=domain.address.value&filter=match:domain.address.standard_id:input1&link=domain.address.standard_id:eq.domain.arp.standard_id
```
