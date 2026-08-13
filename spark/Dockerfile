# The stock apache/spark image ships no pymongo, which the foreachBatch sink
# needs to reach MongoDB. Nothing else is changed: the job itself is still
# bind-mounted at /app, so editing it only needs a container restart.
FROM apache/spark:4.0.0

USER root
RUN pip3 install --no-cache-dir "pymongo==4.*"

# Mount point for the checkpoint volume. Docker initializes a fresh named
# volume from the image directory, ownership included, so creating it here as
# uid 185 is what lets the unprivileged user write to it. It sits outside /app
# because /app is itself a bind mount and nesting the two is fragile.
RUN mkdir -p /checkpoint && chown 185:185 /checkpoint

# Back to the image's unprivileged user (uid 185)
USER 185
