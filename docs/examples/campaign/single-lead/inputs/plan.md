# Plan: bounded defect fix

This file is the frozen plan the lead task works from. Replace it with your own
before submitting; the campaign machinery does not read it, the agent does.

## Objective

Fix the single defect described below, with a test that fails before the fix and
passes after it.

## Defect

Describe the defect here: what is observed, what is expected, and the smallest
reproduction you know of.

## Boundaries

- Change only what the defect requires. Refactoring nearby code is out of scope.
- Do not change the public interface unless the defect is in the interface.
- Do not skip, weaken or delete an existing test to make the suite pass.

## Done means

One commit on the current branch, the full test suite passing at that commit,
and a handoff short enough to read in a minute.
