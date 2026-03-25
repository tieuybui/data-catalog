"""
Password protection with multi-user support.
Users are defined in st.secrets["users"] as email = password pairs.
Login is persisted to browser LocalStorage via JS components.
"""

import hashlib
import hmac
import json
import streamlit as st
import streamlit.components.v1 as components

_LS_KEY = "dd_auth"


def _make_token(username: str, password: str) -> str:
    return hashlib.sha256(f"{username}:{password}".encode()).hexdigest()


def check_password():
    """Block access unless user provides valid credentials."""

    if "users" not in st.secrets:
        return True

    users: dict = dict(st.secrets["users"])

    # Already authenticated this session
    if st.session_state.get("authenticated"):
        return True

    # Logging out — render JS to clear LocalStorage, then show login
    if st.session_state.get("_logging_out"):
        st.set_page_config(page_title="Logging out...", page_icon="🔒")
        components.html(
            f"""<script>
            localStorage.removeItem("{_LS_KEY}");
            // Reload to show clean login page
            window.parent.location.reload();
            </script>""",
            height=0,
        )
        st.session_state.pop("_logging_out", None)
        st.stop()

    # Try auto-login from LocalStorage (passed via hidden input on previous load)
    ls_data = st.session_state.get("_ls_data")
    if ls_data:
        try:
            data = json.loads(ls_data) if isinstance(ls_data, str) else ls_data
            saved_user = data.get("user", "")
            saved_token = data.get("token", "")
            expected_pw = users.get(saved_user)
            if expected_pw and saved_token == _make_token(saved_user, expected_pw):
                st.session_state["authenticated"] = True
                st.session_state["username"] = saved_user
                return True
        except (json.JSONDecodeError, AttributeError):
            pass

    # Show login page
    st.set_page_config(page_title="Login", page_icon="🔒")
    st.title("🔒 Data Dictionary")

    # JS: read LocalStorage → write into a hidden Streamlit text_input → trigger rerun
    components.html(
        f"""<script>
        const data = localStorage.getItem("{_LS_KEY}");
        if (data) {{
            const doc = window.parent.document;
            const el = doc.querySelector('input[aria-label="_ls_data"]');
            if (el) {{
                const nativeSet = Object.getOwnPropertyDescriptor(
                    HTMLInputElement.prototype, 'value').set;
                nativeSet.call(el, data);
                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                // Small delay then trigger form submit
                setTimeout(() => {{
                    el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                }}, 100);
            }}
        }}
        </script>""",
        height=0,
    )

    # Hidden input to receive LocalStorage data from JS
    st.text_input("_ls_data", key="_ls_data", label_visibility="hidden")

    email = st.text_input("Email", key="_email_input", placeholder="you@example.com")
    pw = st.text_input("Password", type="password", key="_password_input")

    if st.button("Login"):
        email = email.strip().lower()
        expected_pw = users.get(email)
        if expected_pw and hmac.compare_digest(pw, expected_pw):
            st.session_state["authenticated"] = True
            st.session_state["username"] = email
            token_data = json.dumps({"user": email, "token": _make_token(email, pw)})
            # Save to LocalStorage via JS
            components.html(
                f"""<script>
                localStorage.setItem("{_LS_KEY}", {json.dumps(token_data)});
                </script>""",
                height=0,
            )
            st.rerun()
        else:
            st.error("Invalid email or password.")

    st.stop()


def logout():
    """Set flag to clear LocalStorage on next render."""
    st.session_state.pop("authenticated", None)
    st.session_state.pop("username", None)
    st.session_state.pop("_ls_data", None)
    st.session_state["_logging_out"] = True
