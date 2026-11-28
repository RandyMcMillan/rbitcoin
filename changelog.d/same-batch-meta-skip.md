Changed

- Structural spentness does not read a spend slot whose parent was created
  in the same confirm batch. That slot is still empty. A parent from an
  earlier batch is still read.
