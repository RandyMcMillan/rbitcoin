Changed

- **A failed append refuses later writes to its files until reopen.** When
  a group of appends fails, a length may already be durable, so every file
  in that group refuses writes until the store reopens; reads continue and
  a flush may still republish the RAM length.
