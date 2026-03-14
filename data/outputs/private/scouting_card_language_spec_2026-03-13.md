# Scouting Card Language Spec (2026-03-13)

## Purpose
This spec defines how public scouting-card copy should be generated so the writing reads like clean, technical football scouting rather than generic praise or vague concern labels.

The goal is not to expose proprietary methodology. The goal is to make the public sections precise, causal, and player-specific.

## Core Doctrine
Every sentence should explain:
1. what happens
2. when it happens
3. why it happens
4. what it means in the NFL

Avoid vibe-based copy. Prefer mechanism-first and condition-first language.

## Three Failure Modes To Prevent
### 1. Relative Terms Without Reference
Bad:
- `good passer`
- `solid athlete`
- `strong route runner`

Rule:
- If the sentence uses a relative term, it must immediately define the football action behind it.

### 2. Descriptive Instead Of Conditional
Bad:
- `technically inconsistent`
- `stiff in coverage`
- `can be late with hands`

Rule:
- State the trigger.
- State the issue.
- State the consequence.

### 3. Labels Instead Of Mechanisms
Bad:
- `high motor`
- `playmaker`
- `creates separation`
- `work rate`

Rule:
- Replace label language with movement, leverage, timing, processing, or fit-based mechanism language.

## Section Templates
### How He Wins
Required structure:
- mechanism
- usage context
- translation outcome

Preferred sentence shape:
- `Wins by [mechanism], especially when [usage/context], which gives him [translation outcome].`

### Primary Concerns
Required structure:
- trigger
- issue
- NFL consequence

Preferred sentence shape:
- `When [trigger], [issue] shows up, which can lead to [NFL consequence].`

### Report Summary
Required structure:
- role
- best usage path
- primary translation driver
- main swing factor

Preferred sentence shape:
- `Projects as [role] with his cleanest NFL path in [usage/scheme]. The translation case is driven by [primary mechanism], while the main swing factor is [primary concern].`

## Restricted Vocabulary
These words should not appear alone in generated public copy unless followed by a mechanism:
- good
- solid
- inconsistent
- explosive
- physical
- instinctive
- high motor
- playmaker
- technically sound
- creates separation
- work rate

## Position-Specific Mechanism Templates
### QB
Use:
- platform stability
- anticipation
- pressure response
- sequencing
- off-platform variance

Examples:
- `Wins when he can stay on schedule, throw windows open, and keep his platform under him through the top of the drop.`
- `When interior pressure compresses the platform, placement can flatten and turn routine completions into contested throws.`

### RB
Use:
- track discipline
- entry-point acceleration
- contact balance
- pass-pro value
- receiving utility

Examples:
- `Wins by pressing the track with patience, then accelerating through narrow entry points without losing contact balance.`
- `When the first picture clouds, he can bounce too quickly and trade efficient north-south yardage for lateral drift.`

### WR / TE
Use:
- release plan
- leverage manipulation
- route pacing
- hands technique
- contact finish

Examples:
- `Wins by pacing stems and forcing corners to turn early, which creates late separation at the breakpoint.`
- `When defenders disrupt timing at the line, the route can get compressed before he re-stacks leverage.`

### OL
Use:
- base
- anchor
- hand timing
- recovery mechanics
- leverage maintenance

Examples:
- `Wins with a controlled base and timely hands that keep rushers on his edges instead of into his chest.`
- `When rushers land first contact into the frame, the anchor can soften and force him into late recovery.`

### EDGE / DL
Use:
- get-off
- rush plan
- counter timing
- bend/pad level
- finish through contact

Examples:
- `Wins by threatening the upfield shoulder early, then cashing that stress into counters once blockers overset.`
- `When the first move stalls, the rush can flatten because the second answer is not always ready on time.`

### LB
Use:
- read/trigger speed
- block navigation
- range
- tackle finish
- coverage spacing or pressure utility

Examples:
- `Wins by seeing the picture early, fitting through traffic on time, and closing space before the run lane fully opens.`
- `When eye candy widens his first step, the fit can get late and stress his tackle angle at the second level.`

### CB / S
Use:
- leverage discipline
- hip transition
- click-and-close
- ball tracking
- tackle/fit reliability

Examples:
- `Wins by staying patient in phase, then clicking and closing once the route declares in front of him.`
- `When he has to open and redirect from off leverage, the transition can get too linear against sharper in-breakers.`

## Generator Order Of Operations
1. manual player notes
2. glossary-tagged public-safe phrases
3. position-specific mechanism templates
4. model-feature fallback

## QA Standard
Before publishing generated copy, ask:
1. Does this sentence explain a football mechanism?
2. Does it identify when the issue or win shows up?
3. Does it imply an NFL consequence rather than just a trait label?
4. Would the sentence still make sense if the adjective were removed?

If the answer to any of the above is no, the sentence needs revision.
