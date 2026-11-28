Fixed

- A confirm that rolls a filter or tweak segment syncs the earlier
  segment's index length before it writes the next segment's index
  length. A crash cannot publish the new segment's rows under the old
  segment's slot count.
- A confirm that seals filters and tweaks on different segment ids
  still publishes both index lengths in one wait when neither table
  rolls.
- A tweak plan that rolls a segment and then does not commit rebases
  that empty segment before the next plan. The next heights stay on the
  slots just written.
