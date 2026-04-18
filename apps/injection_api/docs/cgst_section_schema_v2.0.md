# CGST Section Chunk Type Schema Documentation

## Schema Version: 2.0
**Chunk Type:** `cgst_section`  
**Last Updated:** 2026-04-18  
**Status:** Production

---

## 1. Core Identity Fields

### `id`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Pattern:** `^cgst-s\d+[a-zA-Z]?(-ss[a-zA-Z0-9\(\)]+|-overview)$`
- **Example:** `"cgst-s140-ss1"`, `"cgst-s2-ss49"`, `"cgst-s144-overview"`
- **Description:** Unique identifier for the chunk
- **Validation Rules:**
  - Must be unique across all chunks
  - Format: `cgst-s{section_number}[suffix]-ss{subsection_id}` OR `cgst-s{section_number}-overview`
  - No spaces or special characters except hyphen
- **Usage:** Primary key for retrieval, deduplication, cross-references

### `chunk_type`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Allowed Values:** `"cgst_section"`
- **Description:** Identifies this as a CGST Act section chunk
- **Validation Rules:**
  - Must be exactly `"cgst_section"`
  - Case-sensitive
- **Usage:** Route to correct processing pipeline, filter queries

### `parent_doc`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Allowed Values:** `"Central Goods and Services Tax Act, 2017"`
- **Description:** Source legislation name
- **Validation Rules:**
  - Must match canonical act name exactly
  - Immutable after creation
- **Usage:** Document-level filtering, citation generation

### `chunk_index`
- **Type:** `integer`
- **Required:** ✅ Mandatory
- **Range:** `1` to `total_chunks`
- **Description:** Sequential position within the parent section
- **Validation Rules:**
  - Must be ≥ 1
  - Must be ≤ `total_chunks`
  - No gaps in sequence for same section
- **Usage:** Ordered retrieval, pagination, context assembly

### `total_chunks`
- **Type:** `integer`
- **Required:** ✅ Mandatory
- **Range:** `1` to `999`
- **Description:** Total number of chunks for this section
- **Validation Rules:**
  - Must be ≥ `chunk_index`
  - Should match actual chunk count for section
- **Usage:** Progress tracking, completeness validation

---

## 2. Content Fields

### `text`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Min Length:** 10 characters
- **Max Length:** 50,000 characters
- **Description:** Actual legal provision text verbatim from the Act
- **Validation Rules:**
  - Must not be empty or whitespace-only
  - Must preserve original formatting (line breaks, indentation)
  - No truncation mid-sentence
- **Usage:** Primary content for display, legal reference, exact match search

### `summary`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Min Length:** 20 characters
- **Max Length:** 1,000 characters
- **Description:** AI-generated concise summary of the provision
- **Validation Rules:**
  - Must be human-readable sentence(s)
  - Should not exceed 200 words
  - Must be different from `text` (not verbatim copy)
- **Usage:** Quick preview, semantic search, snippet display

### `keywords`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Min Items:** 3
- **Max Items:** 40
- **Item Type:** `string` (2-50 characters each)
- **Description:** Search-optimized terms and phrases
- **Validation Rules:**
  - No duplicates within array
  - Each keyword: lowercase, alphanumeric + spaces/hyphens only
  - Should include domain-specific terms (e.g., "input tax credit", "registration", "composition scheme")
- **Usage:** Keyword search, query expansion, faceted filtering

---

## 3. Authority Object

### `authority`
- **Type:** `object`
- **Required:** ✅ Mandatory

#### `authority.level`
- **Type:** `integer`
- **Required:** ✅ Mandatory
- **Allowed Values:**
  - `1` = Parliamentary Statute
  - `2` = Subordinate Rules
  - `3` = Notifications
  - `4` = Circulars
  - `5` = Orders
- **Description:** Legal hierarchy rank
- **Validation:** Must be `1` for `cgst_section` chunks
- **Usage:** Authority-based ranking, citation precedence

#### `authority.label`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Allowed Values:** `"Parliamentary Statute"`
- **Description:** Human-readable authority level
- **Validation:** Must be `"Parliamentary Statute"` for `cgst_section`
- **Usage:** Display, filtering

#### `authority.is_statutory`
- **Type:** `boolean`
- **Required:** ✅ Mandatory
- **Allowed Values:** `true`
- **Description:** Whether provision is statutory law
- **Validation:** Must be `true` for `cgst_section`
- **Usage:** Legal weight filtering

#### `authority.is_binding`
- **Type:** `boolean`
- **Required:** ✅ Mandatory
- **Allowed Values:** `true`
- **Description:** Whether provision is legally binding
- **Validation:** Must be `true` for `cgst_section`
- **Usage:** Legal enforceability checks

#### `authority.can_be_cited`
- **Type:** `boolean`
- **Required:** ✅ Mandatory
- **Allowed Values:** `true`
- **Description:** Whether provision can be cited in legal proceedings
- **Validation:** Must be `true` for `cgst_section`
- **Usage:** Citation validity checks

---

## 4. Temporal Object

### `temporal`
- **Type:** `object`
- **Required:** ✅ Mandatory

#### `temporal.effective_date`
- **Type:** `string | null` (Date: `DD-MM-YYYY`)
- **Required:** ✅ Mandatory
- **Pattern:** `^\d{2}-\d{2}-\d{4}$` (if not null)
- **Example:** `"01-07-2017"`
- **Description:** Date when provision became legally effective
- **Validation Rules:**
  - Must be valid date (not future date beyond current ingestion)
  - Format: DD-MM-YYYY
- **Usage:** Temporal filtering, applicability determination

#### `temporal.superseded_date`
- **Type:** `string | null` (ISO 8601 date)
- **Required:** ✅ Mandatory
- **Default:** `null`
- **Description:** Date when provision was replaced/repealed
- **Validation Rules:**
  - If not null, must be > `effective_date`
  - If not null, `is_current` must be `false`
- **Usage:** Historical queries, version control

#### `temporal.is_current`
- **Type:** `boolean`
- **Required:** ✅ Mandatory
- **Description:** Whether provision is currently in force
- **Validation Rules:**
  - If `superseded_date` is not null → must be `false`
  - If `superseded_date` is null → must be `true`
- **Usage:** Filter current vs historical provisions

#### `temporal.financial_year`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Pattern:** `^\d{4}-\d{2}$`
- **Example:** `"2017-18"`, `"2025-26"`
- **Description:** Financial year of enactment/effectiveness
- **Validation Rules:**
  - Format: `YYYY-YY` where YY = last 2 digits of next year
  - Must align with `effective_date`
- **Usage:** FY-based filtering, budget period queries

---

## 5. Legal Status Object

### `legal_status`
- **Type:** `object`
- **Required:** ✅ Mandatory

#### `legal_status.is_disputed`
- **Type:** `boolean`
- **Required:** ✅ Mandatory
- **Default:** `false`
- **Description:** Whether provision is under legal challenge
- **Validation:** If `true`, `dispute_note` must be non-null
- **Usage:** Risk flagging, compliance caution

#### `legal_status.dispute_note`
- **Type:** `string | null`
- **Required:** ✅ Mandatory
- **Max Length:** 500 characters
- **Description:** Brief note on nature of dispute
- **Validation:** Required if `is_disputed = true`
- **Usage:** Context for disputed provisions

#### `legal_status.current_status`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Allowed Values:**
  - `"active"` = Currently enforceable
  - `"repealed"` = No longer valid
  - `"suspended"` = Temporarily not enforced
  - `"stayed"` = Court-ordered halt
  - `"amended"` = Modified (redirect to newer version)
- **Description:** Current legal state
- **Validation:** If `"repealed"` or `"amended"`, `temporal.is_current` should be `false`
- **Usage:** Status-based filtering

#### `legal_status.overruled_by`
- **Type:** `string | null`
- **Required:** ✅ Mandatory
- **Pattern:** `^(igst|cgst|ugst|sgst)-s\d+.*$` (if not null)
- **Description:** Chunk ID that supersedes this provision
- **Validation:** Must reference valid chunk ID if not null
- **Usage:** Version chain tracking

---

## 6. Cross References Object

### `cross_references`
- **Type:** `object`
- **Required:** ✅ Mandatory

#### `cross_references.sections`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Default:** `[]`
- **Item Pattern:** `^Section \d+[A-Z]?$`
- **Example:** `["Section 140", "Section 7"]`
- **Description:** Related sections in 'Section X' format.
- **Usage:** Legal navigation, context assembly

#### `cross_references.rules`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Default:** `[]`
- **Item Pattern:** `^Rule \d+[A-Z]?$`
- **Example:** `["Rule 117", "Rule 36"]`
- **Description:** Related CGST Rules in 'Rule X' format.
- **Usage:** Procedural cross-linking

#### `cross_references.notifications`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Default:** `[]`
- **Item Pattern:** `^\d+/\d+(-(CT|ST|IT|UT))?(\(Rate\))?$`
- **Example:** `["01/2017-CT(Rate)", "10/2023-CT"]`
- **Description:** Related notification numbers.
- **Usage:** Exemption/clarification linking

#### `cross_references.circulars`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Default:** `[]`
- **Description:** Related circular numbers/IDs
- **Usage:** Interpretive guidance linking

#### `cross_references.forms`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Default:** `[]`
- **Item Pattern:** `^(GSTR|GST )(ITC-|REG-|RFD-)?\d+[A-Z]?$`
- **Description:** Related GST forms
- **Usage:** Compliance workflow linking

#### `cross_references.hsn_codes`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Default:** `[]`
- **Item Pattern:** `^\d{4,8}$`
- **Description:** Related HSN codes for goods
- **Usage:** Product classification linking

#### `cross_references.sac_codes`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Default:** `[]`
- **Item Pattern:** `^\d{6}$`
- **Description:** Related SAC codes for services
- **Usage:** Service classification linking

#### `cross_references.judgment_ids`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Default:** `[]`
- **Description:** IDs of judgments interpreting this provision
- **Usage:** Case law linking

#### `cross_references.parent_chunk_id`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Pattern:** `^cgst-s\d+[a-zA-Z]?-overview$`
- **Description:** ID of overview chunk for this section
- **Validation Rules:**
  - Must reference existing chunk
  - For overview chunks, should self-reference
- **Usage:** Section-level aggregation, navigation

---

## 7. Retrieval Object

### `retrieval`
- **Type:** `object`
- **Required:** ✅ Mandatory

#### `retrieval.primary_topics`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Min Items:** 1
- **Max Items:** 10
- **Allowed Values (enum):**
  - `"input_tax_credit"`
  - `"registration"`
  - `"returns"`
  - `"payment"`
  - `"refund"`
  - `"assessment"`
  - `"audit"`
  - `"appeals"`
  - `"penalties"`
  - `"interest"`
  - `"composition_scheme"`
  - `"definitions"`
  - `"transitional_provisions"`
  - `"exemptions"`
  - `"valuation"`
  - `"time_of_supply"`
  - `"anti_profiteering"`
  - `"accounts_records"`
  - `"tax_invoice"`
  - `"job_work"`
  - `"electronic_commerce"`
  - `"assessment_audit"`
  - `"demands_recovery"`
  - `"offences_penalties"`
  - `"levy_collection"`
  - `"scope_of_supply"`
- **Description:** High-level thematic categories
- **Usage:** Topic-based filtering, recommendation

#### `retrieval.tax_type`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Allowed Values:** `"CGST"`
- **Description:** Tax regime identifier
- **Validation:** Must be `"CGST"` for `cgst_section` chunks
- **Usage:** Tax type filtering

#### `retrieval.applicable_to`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Allowed Values (enum):**
  - `"goods"` = Applies only to goods
  - `"services"` = Applies only to services
  - `"both"` = Applies to goods and services
- **Description:** Scope of applicability
- **Validation:** Must be one of the three enum values
- **Usage:** Goods/services filtering

#### `retrieval.query_categories`
- **Type:** `array[string]`
- **Required:** ✅ Mandatory
- **Min Items:** 1
- **Max Items:** 8
- **Allowed Values (enum):**
  - `"compliance"` = How to comply
  - `"procedure"` = Step-by-step process
  - `"definition"` = What is X
  - `"applicability"` = Does this apply to me
  - `"calculation"` = How to compute
  - `"documentation"` = What documents needed
  - `"timeline"` = When is due date
  - `"penalty"` = Consequences of non-compliance
  - `"exemption"` = Who is exempt
  - `"rate"` = What is tax rate
  - `"refund"` = How to get refund
  - `"transitional"` = Migration rules
- **Description:** User intent categories this chunk answers
- **Usage:** Intent-based retrieval ranking

#### `retrieval.boost_score`
- **Type:** `float`
- **Required:** ✅ Mandatory
- **Range:** `0.0` to `1.0`
- **Decimals:** Up to 2 decimal places
- **Description:** Relevance multiplier for search ranking
- **Scoring Logic:**
  - `1.0` = Critical provisions (definitions, key procedures)
  - `0.9-0.95` = High-impact provisions (core compliance, ITC)
  - `0.8-0.89` = Moderate provisions (conditional rules)
  - `0.7-0.79` = Supporting provisions (edge cases)
  - `0.5-0.69` = Procedural/administrative
  - `< 0.5` = Ancillary references
- **Usage:** Score multiplier in vector search

---

## 8. Provenance Object

### `provenance`
- **Type:** `object`
- **Required:** ✅ Mandatory

#### `provenance.source_file`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Pattern:** `^[\w\-]+\.(json|pdf|txt)$`
- **Example:** `"cgst_act_2017.json"`
- **Description:** Original source file name
- **Usage:** Audit trail, re-ingestion reference

#### `provenance.page_range`
- **Type:** `string | null`
- **Required:** ✅ Mandatory
- **Pattern:** `^\d+(-\d+)?$` (if not null)
- **Example:** `"45"`, `"45-47"`, `null`
- **Description:** Page numbers in source PDF (if applicable)
- **Usage:** Physical source reference

#### `provenance.ingestion_date`
- **Type:** `string` (ISO 8601 date)
- **Required:** ✅ Mandatory
- **Pattern:** `^\d{4}-\d{2}-\d{2}$`
- **Description:** Date chunk was created/ingested
- **Validation:** Cannot be future date
- **Usage:** Data freshness tracking

#### `provenance.version`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Pattern:** `^\d+\.\d+$`
- **Example:** `"1.0"`, `"2.1"`
- **Description:** Chunk schema/content version
- **Usage:** Version migration, compatibility

#### `provenance.last_updated`
- **Type:** `string` (ISO 8601 datetime)
- **Required:** ⚠️ Recommended
- **Pattern:** `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`
- **Description:** Last modification timestamp
- **Usage:** Change tracking, cache invalidation

#### `provenance.updated_by`
- **Type:** `string`
- **Required:** ⚠️ Recommended
- **Allowed Values:** `"auto"` | `"human:{user_id}"` | `"llm:{model_name}"`
- **Description:** Entity that last modified chunk
- **Usage:** Audit trail, quality control

---

## 9. Ext Object (CGST-Specific Extensions)

### `ext`
- **Type:** `object`
- **Required:** ✅ Mandatory

#### `ext.act`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Allowed Values:** `"CGST Act, 2017"`
- **Description:** Act short name
- **Usage:** Display, citation

#### `ext.chapter_number`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Pattern:** `^(I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV|XVI|XVII|XVIII|XIX|XX|XXI)$` (Roman numerals)
- **Example:** `"XII"`, `"I"`
- **Description:** Chapter number in Act
- **Usage:** Structural navigation

#### `ext.chapter_title`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Max Length:** 200 characters
- **Example:** `"Miscellaneous"`, `"Preliminary"`
- **Description:** Chapter heading
- **Usage:** Context display

#### `ext.section_number`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Pattern:** `^\d+[A-Z]?$`
- **Example:** `"140"`, `"2"`, `"109"`
- **Description:** Section number
- **Validation:** Must match `id` pattern
- **Usage:** Citation, sorting

#### `ext.section_title`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Max Length:** 300 characters
- **Example:** `"Transitional arrangements for input tax credit."`
- **Description:** Section heading
- **Usage:** Display, navigation

#### `ext.sub_section`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Pattern:** `^(\(\d+[A-Z]?\)|overview|Explanation|Illustration|\([a-z]{1,2}\))$`
- **Example:** `"(1)"`, `"overview"`, `"(49)"`
- **Description:** Sub-section identifier
- **Validation:** Must match `id` pattern
- **Usage:** Precise reference

#### `ext.hierarchy_level`
- **Type:** `integer`
- **Required:** ✅ Mandatory
- **Allowed Values:**
  - `3` = Section-level (Overview/Main section)
  - `4` = Sub-section level (Numbered parts e.g., (1), (2))
  - `5` = Clause level (Lettered parts e.g., (a), (b))
  - `6` = Sub-clause level (Roman numerals e.g., (i), (ii))
- **Description:** Structural depth of the legal provision
- **Usage:** Navigation, indentation, and retrieval granularity

#### `ext.provision_type`
- **Type:** `string`
- **Required:** ✅ Mandatory
- **Allowed Values (enum):**
  - `"commencement"` = Effective date/start
  - `"extent"` = Territorial jurisdiction
  - `"definition"` = Legal terminology
  - `"administrative"` = Officers and powers
  - `"levy"` = Charging provisions
  - `"composition_scheme"` = Composition levy rules
  - `"input_tax_credit"` = ITC eligibility and conditions
  - `"registration"` = Registration procedures
  - `"tax_invoice"` = Invoice, credit and debit notes
  - `"accounts_records"` = Maintenance of records
  - `"returns"` = Filing of returns
  - `"payment"` = Payment of tax
  - `"refund"` = Refund procedures
  - `"assessment"` = Self-assessment and other assessments
  - `"audit"` = Audit by authorities
  - `"inspection_search_seizure"` = Enforcement powers
  - `"demands_recovery"` = Demand and recovery of tax
  - `"liability_to_pay"` = Liability in certain cases
  - `"advance_ruling"` = Advance ruling mechanism
  - `"appeals_revision"` = Appeals and revision procedures
  - `"offences_penalties"` = Penalties and prosecution
  - `"transitional"` = Migration and transitional rules
  - `"miscellaneous"` = Other provisions
  - `"procedure"` = General procedures
  - `"condition"` = Conditional provisions
- **Description:** Functional category
- **Usage:** Provision type filtering

#### `ext.has_proviso`
- **Type:** `boolean`
- **Required:** ✅ Mandatory
- **Description:** Whether provision contains "Provided that" clause
- **Validation:** If `true`, `proviso_text` must be non-null
- **Usage:** Exception handling flag

#### `ext.proviso_text`
- **Type:** `string | null`
- **Required:** ✅ Mandatory
- **Max Length:** 10,000 characters
- **Description:** Full text of proviso clause(s)
- **Validation:** Required if `has_proviso = true`
- **Usage:** Exception retrieval

#### `ext.proviso_implication`
- **Type:** `string | null`
- **Required:** ✅ Mandatory
- **Max Length:** 1,000 characters
- **Description:** AI-generated summary of proviso impact
- **Validation:** Required if `has_proviso = true`
- **Usage:** Quick exception understanding

#### `ext.has_explanation`
- **Type:** `boolean`
- **Required:** ✅ Mandatory
- **Description:** Whether provision has Explanation clause
- **Usage:** Supplementary content flag

#### `ext.explanation_text`
- **Type:** `string | null`
- **Required:** ⚠️ Recommended
- **Max Length:** 10,000 characters
- **Description:** Full Explanation text
- **Validation:** Required if `has_explanation = true`
- **Usage:** Clarification retrieval

#### `ext.has_illustration`
- **Type:** `boolean`
- **Required:** ✅ Mandatory
- **Description:** Whether provision has Illustration
- **Usage:** Example content flag

#### `ext.illustration_text`
- **Type:** `string | null`
- **Required:** ⚠️ Recommended
- **Max Length:** 10,000 characters
- **Description:** Full Illustration text
- **Validation:** Required if `has_illustration = true`
- **Usage:** Example retrieval

#### `ext.cgst_specific`
- **Type:** `object`
- **Required:** ✅ Mandatory
- **Description:** CGST-only metadata (future extensions)
- **Current Fields:** (empty - reserved for future)
- **Potential Future Fields:**
  - `is_compensation_cess_applicable` (bool)
  - `state_specific_applicability` (bool)

#### `ext.amendment_history`
- **Type:** `array[object]`
- **Required:** ✅ Mandatory
- **Default:** `[]`
- **Item Schema:**
  ```json
  {
    "amendment_act": "string",           // e.g., "Finance Act, 2023"
    "amendment_date": "YYYY-MM-DD",      // ISO date
    "effective_from": "YYYY-MM-DD",      // ISO date
    "amendment_type": "insertion|substitution|omission|addition",
    "old_text": "string | null",         // Text before amendment
    "change_summary": "string",          // What changed
    "notification_no": "string | null",  // If notified separately
    "is_retrospective": boolean
  }
  ```
- **Description:** Complete amendment trail
- **Validation:** Ordered by `amendment_date` ascending
- **Usage:** Version history, temporal queries

---

## 10. Validation Rules (Schema-Wide)

### Mandatory Validation Checks
1. **ID Uniqueness:** No two chunks can have same `id`
2. **Parent Reference:** `cross_references.parent_chunk_id` must exist in database
3. **Temporal Consistency:**
   - `effective_date` ≤ `superseded_date` (if not null)
   - If `superseded_date` exists → `is_current = false`
4. **Authority Consistency:** All authority fields must be `true` and `level=1` for `cgst_section`
5. **Conditional Requirements:**
   - `has_proviso = true` → `proviso_text` and `proviso_implication` must be non-null
   - `has_explanation = true` → `explanation_text` must be non-null
   - `has_illustration = true` → `illustration_text` must be non-null
   - `is_disputed = true` → `dispute_note` must be non-null
6. **Array Constraints:**
   - `keywords`: 3-30 items
   - `primary_topics`: 1-10 items
   - `query_categories`: 1-8 items
7. **Enum Validation:** All enum fields must match exactly (case-sensitive)

### Data Quality Checks
1. **Text vs Summary:** Summary must not be verbatim copy of text
2. **Keyword Relevance:** Keywords should capture legal terminology used in the provision
3. **Boost Score Logic:** Core sections and definitions should have higher boost scores (0.9+)
4. **Amendment History Order:** Sorted chronologically
5. **Cross-Reference Validity:** Referenced section/rule IDs should follow standard patterns

---

## 11. Index Recommendations

### Primary Indexes
- `id` (unique)
- `chunk_type` + `temporal.is_current`
- `ext.section_number` + `ext.sub_section`

### Search Indexes
- `keywords` (array index)
- `retrieval.primary_topics` (array index)
- `retrieval.query_categories` (array index)
- `text` (full-text search)
- `summary` (full-text search)

### Filter Indexes
- `temporal.effective_date`
- `retrieval.applicable_to`
- `ext.provision_type`
- `legal_status.current_status`
