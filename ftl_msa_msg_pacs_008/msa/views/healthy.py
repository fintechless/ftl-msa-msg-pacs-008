"""
Flask view for the MSG PACS 008 blueprint
Path: /
"""

from flask import Response
from flask import make_response
from flask import session
from ftl_python_lib.core.context.request import REQUEST_CONTEXT_SESSION
from ftl_python_lib.core.context.request import RequestContext
from ftl_python_lib.core.log import LOGGER

from ftl_msa_msg_pacs_008.msa.blueprints.msg_pacs_008 import BLUEPRINT_MSG_PACS_008


@BLUEPRINT_MSG_PACS_008.route("_healthy", methods=["GET"])
def healthy() -> Response:
    """
    Process GET request for the /msa/pacs-008/_healthy endpoint
    Dummy GET for ELB healthcheck
    """

    request_context: RequestContext = session.get(REQUEST_CONTEXT_SESSION)

    LOGGER.logger.debug("Proccessing GET request for MSG PACS 008 microservice")
    LOGGER.logger.debug(f"Request ID is {request_context.request_id}")
    LOGGER.logger.debug(
        f"Request timestamp is {request_context.requested_at_isoformat}"
    )

    return make_response(
        {
            "request_id": request_context.request_id,
            "status": "OK",
            "message": "Healthy",
        },
        200,
    )
