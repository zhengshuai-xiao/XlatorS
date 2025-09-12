# If command starts with an option, prepend xlators.
if [ "${1}" != "xlators" ]; then
    if [ -n "${1}" ]; then
        set -- xlators "$@"
    fi
fi

## Look for docker secrets at given absolute path or in default documented location.
docker_secrets_env() {
    if [ -f "$MINIO_ACCESS_KEY_FILE" ]; then
        ACCESS_KEY_FILE="$MINIO_ACCESS_KEY_FILE"
    else
        ACCESS_KEY_FILE="/run/secrets/$MINIO_ACCESS_KEY_FILE"
    fi
    if [ -f "$MINIO_SECRET_KEY_FILE" ]; then
        SECRET_KEY_FILE="$MINIO_SECRET_KEY_FILE"
    else
        SECRET_KEY_FILE="/run/secrets/$MINIO_SECRET_KEY_FILE"
    fi

    if [ -f "$ACCESS_KEY_FILE" ] && [ -f "$SECRET_KEY_FILE" ]; then
        if [ -f "$ACCESS_KEY_FILE" ]; then
            MINIO_ACCESS_KEY="$(cat "$ACCESS_KEY_FILE")"
            export MINIO_ACCESS_KEY
        fi
        if [ -f "$SECRET_KEY_FILE" ]; then
            MINIO_SECRET_KEY="$(cat "$SECRET_KEY_FILE")"
            export MINIO_SECRET_KEY
        fi
    fi
}

## Set access env from secrets if necessary.
docker_secrets_env