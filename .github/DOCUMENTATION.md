# nautilus documentation conventions

The help pages should read as though one person wrote them, for a field ecologist rather than for a
package developer. This note records the decisions that make that possible, so that the last page
rewritten matches the first.

It is a working note for contributors, not shipped documentation.

---

## 1. Terminology

| Use | Not | Why |
|---|---|---|
| **deployment** | individual, dataset, record | One tag, on one animal, for one attachment period. It is the unit the package operates on: a row of the metadata table, an element of a list, one `.rds` file, one row of a summary. |
| **the animal** | the individual | Only where the biological subject is genuinely meant - "the animal was diving", not "the animal's data table". |
| **sampling rate** | sampling frequency | Reserve "frequency" for the quantities being measured, so that "frequency" in a tail-beat page is never ambiguous. |
| **band-pass**, **low-pass**, **high-pass** | bandpass, band pass | Hyphenated. |
| **timestamp** | datetime (in prose) | `datetime` is a column name; write it in backticks. In prose, say timestamp. |
| **tag** | logger, device | Except where the distinction matters: a *package* is the housing, a *logger* is the recording unit inside it. Both are defined where they are first used. |

**Spelling is British in prose** - colour, standardise, normalise, behaviour, analyse. Code identifiers
keep their existing spelling and are never "corrected": `summarizeTagData()`, `regularizeTimeSeries()`,
`color.by`, `center`. Write them in backticks so the mismatch reads as a name, not an error.

**Address the reader as "you".** Not "the user", not "the caller".

---

## 2. Page structure

Every exported function has, in this order:

```
title
@description
@details        (where there is methodology to explain)
@param          (one per argument, in signature order)
@return
@seealso
@references     (only where a methodological claim needs support)
@examples
@export
```

**Title** - sentence case, no trailing full stop, states what the function does for the reader:
`Import archival tag data into a standard structure`, not `Import and Standardize Archival Tag Data`.

**Description** - two or three short paragraphs, in this order:

1. the scientific or practical problem;
2. why the function exists, and when a researcher reaches for it;
3. what it does and returns.

Never open with implementation. `Computes...`, `Applies a Butterworth filter...` and
`Provides an end-to-end solution...` are all wrong openings.

**Details** - the home for methodology: algorithmic choices, how a threshold was arrived at, internal
metrics, assumptions, limitations, and any behaviour that would surprise. Break it with `##`
subheadings whenever it runs past two paragraphs. Prefer headings a researcher would recognise
("What is checked", "Tags without a magnetometer") over implementation labels ("Internals", "The
algorithm").

**Arguments** - say what the parameter represents, when you would change it, and what changes as a
result. Not how the algorithm consumes it.

> `timezone` The time zone the tag's clock was set to (default `"UTC"`). Timestamps are labelled with
> this zone, never shifted by it. Set it only if the tag logged local time: an incorrect zone places
> the record at the wrong point in the solar day and will bias any analysis of diel behaviour.

The last clause is the point. An argument description that does not help someone decide has not
finished.

**Return** - name the class and say what it carries. If the return type depends on an argument, say so
in the same sentence.

**See also** - annotated, not a bare list: `[buildTagData()] for data already in R; [processTagData()]
for the next step.`

---

## 3. Markup

- Cross-references: `[functionName()]`. Not `\code{\link{}}`, not bare `\link{}`.
- Code, column names, argument names and values: backticks. Not `\code{}`.
- Emphasis: `**bold**` and `*italic*`. Not `\strong{}` / `\emph{}`.
- Lists: markdown `-`. Use `\describe{}` only for genuine term-and-definition blocks.
- Maths: `\eqn{}` / `\deqn{}`, which stay ASCII.

**Keep source ASCII wherever a plain equivalent exists.** Write `-` for a dash, not an em dash, and
`\eqn{\mu}T` rather than the micro sign. Check with:

```
perl -ne 'print if /[^[:ascii:]]/' R/*.R
```

This applies to the whole of `R/`, roxygen included. `R CMD check` reports non-ASCII in R files, and a
literal symbol in a string also makes the value locale-dependent, which breaks tests under
`R CMD check` even when they pass under `pkgload`.

The escape differs by context, and mixing them up is easy:

- In R **code**, write `"\u00b0"`. R parses the escape at load time and the string carries the symbol.
- In **roxygen**, `\uXXXX` does not work - Rd does not interpret it, and `R CMD check` reports
  `unknown macro`. Use an Rd escape such as `\eqn{\mu}`, or rephrase to avoid the symbol.

Be careful editing files where the same text appears in both a roxygen line and a code string: a global
search-and-replace will silently convert a correct code escape into a literal.

---

## 4. Tone

- No ALL-CAPS emphasis. If a point needs weight, write a sentence that carries it.
- No developer asides: what the code used to do, which version changed it, what a past bug was, or
  what the maintainer measured on private data. Methodological findings that justify a default belong
  in Details, stated as fact and without the biography.
- No defensiveness. State the limitation plainly and move on.
- Bold sparingly, for a genuine hazard - the kind that produces a wrong answer silently.
- Prefer the plain word: "reads" over "ingests", "writes" over "streams", "checks" over "validates
  against a schema".

---

## 5. Recurring text

Reuse these verbatim so that fifty pages agree.

**verbose**
> How much detail to print: `0`/`"quiet"`, `1`/`"normal"`, or `2`/`"detailed"` (default).

**id.col / datetime.col**
> Which column identifies the animal (default `"ID"`).
> Which column holds the timestamps (default `"datetime"`).

**data** (pipeline functions accepting several input shapes)
> A tag object, a list of them, a single table with an `id.col`, or a character vector of `.rds`
> paths. Paths are read one deployment at a time, so a fleet too large for memory can be processed
> without ever holding it all.

**output.dir / return.data / output.suffix / compress** - as written on [importTagData()].

**Control objects**
> A control object from [xControl()] governing ... . Pass `xControl(...)` to change it.

Control constructors document their own arguments; the function that takes one links to it rather
than restating the fields.

---

## 6. References

Only where a citation supports a methodological claim - an algorithm, a statistical method, a
threshold taken from the literature, or a biological convention. Never for completeness.

Cite in the Details paragraph that makes the claim as well as listing it under `@references`, so the
reader sees which sentence it supports.

Format: `Author AB, Author CD (Year) Title. *Journal* Volume:Pages. \doi{...}`. Use the article number
for journals that use one (`9:23`), consistently across every page citing that paper.

**Every citation must be a real publication, checked.** A fabricated reference in a CRAN package is a
serious defect.

---

## 7. Examples

Every exported function has one. Prefer a runnable example over `\dontrun{}`; where the function needs
files, a tag or a fitted object, `\dontrun{}` is correct, but the objects it references must be
constructed in the example or obviously named.

Show the common case first, then at most one variation that answers a real question - "what if my
fleet is too large for memory?", not "here is every argument".
