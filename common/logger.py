def log(event_type, message, **data):
    print(json.dumps({
        "event": event_type,
        "message": message,
        "data": data
    }))