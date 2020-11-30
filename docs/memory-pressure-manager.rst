Integration with Satella's MemoryPressureManager
================================================

This library integrates itself with satella's MemoryPressureManager_.

.. _MemoryPressureManager: https://satella.readthedocs.io/en/latest/instrumentation/memory.html

It will close the non-required chunks when remaining in severity 1 each 30 seconds.

To attach a MPM to a database, use
:meth:`tempsdb.database.Database.register_memory_pressure_manager`.

Series will automatically inherit the parent database's `MemoryPressureManager`.

