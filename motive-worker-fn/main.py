import os
import hmac
import hashlib
import json
import logging
from google.cloud import pubsub_v1
from flask import Response

PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('GCP_PROJECT')
TOPIC = os.getenv('PUBSUB_TOPIC', 'motive-webhooks')
WEBHOOK_SECRET = os.getenv('MOTIVE_WEBHOOK_SECRET')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT, TOPIC)

def verify_signature(raw_body: bytes, header_sig: str) -> bool:
    if not header_sig or not WEBHOOK_SECRET:
        return False
    cleaned = header_sig
    if cleaned.lower().startswith("sha1="):
        cleaned = cleaned.split("=", 1)[1]
    computed = hmac.new(WEBHOOK_SECRET.encode('utf-8'), raw_body, hashlib.sha1).hexdigest()
    return hmac.compare_digest(computed, cleaned)

def make_idempotency_key(payload_bytes: bytes, payload_obj: dict):
    if isinstance(payload_obj, dict):
        eid = payload_obj.get('id') or payload_obj.get('event_id') or payload_obj.get('eventId')
        if eid:
            return str(eid)
    return hashlib.sha256(payload_bytes).hexdigest()

# This is the native Google Cloud Function handler
def webhook_handler(request):
    try:
        raw_body = request.get_data()
    except Exception:
        logging.exception("Failed to read request body")
        return Response('Bad request', status=400)

    signature = request.headers.get('X-KT-Webhook-Signature') or request.headers.get('x-kt-webhook-signature')
    if not verify_signature(raw_body, signature):
        logging.warning("Invalid signature")
        return Response('Invalid signature', status=403)

    try:
        payload_obj = json.loads(raw_body.decode('utf-8'))
    except Exception:
        payload_obj = None

    idempotency_key = make_idempotency_key(raw_body, payload_obj or {})
    event_id = ''
    action = ''
    if isinstance(payload_obj, dict):
        event_id = str(payload_obj.get('id','') or payload_obj.get('event_id','') or '')
        action = str(payload_obj.get('action','') or payload_obj.get('type','') or '')

    try:
        attributes = {
            'event_id': event_id,
            'action': action,
            'idempotency_key': idempotency_key
        }
        # publish raw bytes and wait for it to finish sending
        future = publisher.publish(topic_path, raw_body, **attributes)
        future.result() 
    except Exception:
        logging.exception("Failed to publish to Pub/Sub")

    return Response('OK', status=200)
