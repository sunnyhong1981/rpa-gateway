"""Shared pilot routing predicate. Files/notifications remain on the existing transport."""

def eligible_reply(reply):
    target=reply.get('reply_target') or {}
    if not target.get('source_message_id') or not target.get('sender_id') or target.get('require_native_mention'):
        return False
    document=reply.get('document')
    return not document or document.get('image_batch') is True or document.get('image') is True


def eligible_source(message):
    attrs=message.get('attributes') or {}
    if message.get('message_type')!=1 or message.get('private') or attrs.get('deleted'):
        return False
    if attrs.get('rpa_group_push_id') or attrs.get('rpa_notification_recipient'):
        return False
    if not isinstance(attrs.get('in_reply_to'),int) or not attrs.get('rpa_recipient_id'):
        return False
    attachments=message.get('attachments') or []
    return not attachments or ((attrs.get('rpa_pod') or {}).get('provider')=='jsexp'
        and 1<=len(attachments)<=9 and all(a.get('file_type')=='image' for a in attachments))


def source_fingerprint(message):
    import hashlib,json
    attrs=message.get('attributes') or {}
    values=[message.get('id'),message.get('content'),message.get('message_type'),message.get('private'),
            attrs.get('in_reply_to'),attrs.get('rpa_recipient_id'),attrs.get('deleted'),
            [{key:item.get(key) for key in ('id','file_type','size','md5')} for item in message.get('attachments',[])]]
    return hashlib.sha256(json.dumps(values,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def validate_live_source(dsn,payload):
    import psycopg
    from psycopg.rows import dict_row
    from fastapi import HTTPException
    context=payload['context']
    with psycopg.connect(dsn,row_factory=dict_row,connect_timeout=3) as db:
        row=db.execute('SELECT * FROM rpa_gateway_source.test_messages WHERE id=%s',(context['chatwoot_message_id'],)).fetchone()
    if row is None or not eligible_source(row) or source_fingerprint(row)!=context['source_fingerprint']:
        raise HTTPException(409,'Official source changed before submission; no native Enter allowed')
