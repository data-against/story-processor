# Query Syntax Guide: Mastering Search Techniques

When conducting advanced searches, the structure of your query can make a significant difference in the relevance and quality of your results. This guide will walk you through the process of building precise queries, followed by examples of common queries and what they are searching for.

## Step-by-Step Instructions for Assembling a Query

### 1. Identify Key Search Terms

The first step in creating a query is identifying the essential keywords and phrases that will lead to the desired search results. This includes specific terms related to the topic you're researching.

**Example:**
You want to find cases involving unsolved murders of Indigenous women using genetic genealogy.

Key terms: `murder`, `woman`, `Indigenous`, `genetic genealogy`

### 2. Use Boolean Operators

Boolean operators (AND, OR, NOT) allow you to refine your search by combining or excluding terms.

* **AND**: Returns results that contain all specified terms
* **OR**: Expands the search to include results containing any of the terms listed
* **NOT**: Excludes specific terms

**Example:**
You want results that include murder-related terms and genetic genealogy, but also include variations on gender and identity:

```
(murder OR homicide OR femicide OR feminicide) AND (woman OR girl OR Indigenous) AND "genetic genealogy"
```

### 3. Add Phrase Searches

To find exact phrases, use quotation marks around the phrase you're looking for.

**Example:**
You want results with the specific phrase "genetic genealogy":

```
"genetic genealogy"
```

### 4. Include Wildcards

Use wildcards (*) to capture variations of a word. The * wildcard represents any number of characters.

**Example:**
Searching for variations of woman and girl, you can use wom*n or girl* to include results like women or girls:

```
(woman OR women OR girl*)
```

### 5. Group Terms with Parentheses

Parentheses help group terms and clarify the order of operations in complex queries.

**Example:**
You want to search for various violent acts alongside gender identities. Group the terms to maintain clarity:

```
(murder OR homicide OR femicide) AND (woman OR girl OR Indigenous)
```

### 6. Combine and Refine

Now, combine all elements to create a more targeted query that captures all relevant terms and excludes irrelevant information.

**Final Example Query:**
```
((murder OR homicide OR femicide OR feminicide) AND (woman OR girl OR Indigenous) AND "genetic genealogy")
```

## Most Common Queries

Below are common types of searches that are widely used for various research purposes:

### Phrase Search
Use quotes to search for an exact phrase:
```
"climate change"
```

### Exclusion Search
Use NOT or a minus sign to exclude specific terms:
```
Biden NOT Trump
```

### Wildcard Search
Use * to include variations of a term:
```
wom*n
```

### Boolean Logic
Combine multiple terms with AND, OR, or NOT for a precise query:
```
(LGBTQ OR queer) AND "mental health"
```

### Field-Specific Search
Narrow down results to specific fields (like title or content):
```
title: "data science"
```

## Example Queries and What They Are Searching For

### 1. Searching for Unsolved Murder Cases Involving Women or Marginalized Groups

**Query Objective:**
This query looks for articles or reports related to unsolved or cold cases involving murders of women, girls, transgender individuals, nonbinary people, Indigenous people, or other marginalized identities. It also seeks cases where genetic genealogy might have been used to solve the crime.

**Final Query:**
```
((murder OR homicide OR femicide OR feminicide OR murdered OR dead OR death OR killed OR shot OR stabbed OR struck OR strangled OR "life-less") AND (woman OR girl OR transgender OR trans OR nonbinary OR non-binary OR Indigenous OR "Native American" OR two-spirit OR "two spirit" OR "a young woman" OR "a teenage girl" OR "a girl" OR "body of a woman" OR prostitute OR "sex worker") AND ("cold case" OR "genetic genealogy"))
```
**Groupings:**
- First group captures various ways of describing death or violent acts
- Second group identifies potential victims using inclusive terminology
- Third group narrows results to cold cases or those using genetic genealogy

### 2. Searching for Violent Deaths of Women and Marginalized Groups

**Query Objective:**
This query is designed to find reports involving violent deaths or unnatural deaths of women, transgender people, nonbinary individuals, and Indigenous individuals. The query includes various forms of death (murder, homicide, etc.) and captures marginalized gender identities.

**Final Query:**
```
((murder* OR homicide* OR femicide* OR feminicide* OR murdered OR dead OR death OR killed OR murdered OR shot OR stabbed OR struck OR strangled OR "life-less" OR hypothermia OR suicide) AND (woman OR women OR girl* OR transgender OR trans OR nonbinary OR non-binary OR Indigenous OR "Native American" OR two-spirit OR "two spirit" OR prostitute OR "sex worker"))
```
**Groupings:**
- First group uses wildcards (murder*) to capture variations like "murdered," "murders," "murderous"
- Second group includes both singular and plural forms of identities
- Terms are ordered from most to least common to optimize search performance

