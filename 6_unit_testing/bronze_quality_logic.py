def quarantine_rule(rules: dict) -> str:
    """
    True if ANY rule failed — the row belongs in quarantine.

    An empty rules dict is a real edge case, not a hypothetical one — it would
    previously have produced "NOT()", which is invalid SQL, not "nothing
    fails," the way an empty dict might look like it should behave. Guarded
    explicitly rather than left to fail confusingly deep inside a pipeline run.
    """
    if not rules:
        raise ValueError("quarantine_rule() requires at least one rule — an empty dict would produce invalid SQL (\"NOT()\").")
    return "NOT({0})".format(" AND ".join(rules.values()))
