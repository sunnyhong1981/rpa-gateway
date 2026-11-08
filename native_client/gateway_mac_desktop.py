"""Keep current Mac checks; fence and then recheck the final native send Enter."""
from mac_desktop import MacDesktop,DesktopNotReady


class GatewayMacDesktop(MacDesktop):
    def _verify_gateway_desktop_release(self):
        bridge=getattr(self,'gateway_bridge',None)
        if bridge is None or bridge.active_gateway_message is None:return
        _,target,field=self._gateway_prepared
        if self.window()!=target or self._input_value(field).strip():
            raise DesktopNotReady('Native submit returned but group/input is not safely released')
        self._image_rows(target)
        bridge.verify_desktop_submission()

    def send(self,*args,**kwargs):
        result=super().send(*args,**kwargs)
        self._verify_gateway_desktop_release()
        return result

    def send_images(self,*args,**kwargs):
        result=super().send_images(*args,**kwargs)
        self._verify_gateway_desktop_release()
        return result

    def ready_to_send(self):
        prepared=super().ready_to_send()
        self._gateway_prepared=prepared
        self._gateway_batch_check=None
        return prepared

    def stage_mention(self,*args,**kwargs):
        # Picker Enter selects a person, never a send. Keep it outside the task fence.
        self._gateway_picker_depth=getattr(self,'_gateway_picker_depth',0)+1
        try:return super().stage_mention(*args,**kwargs)
        finally:self._gateway_picker_depth-=1

    def _batch_draft_matches(self,*args,**kwargs):
        result=super()._batch_draft_matches(*args,**kwargs)
        if result:self._gateway_batch_check=(args,kwargs)
        return result

    def _press(self,pid,code):
        bridge=getattr(self,'gateway_bridge',None)
        if (code==36 and not getattr(self,'_gateway_picker_depth',0) and bridge is not None
                and bridge.active_gateway_message is not None):
            _,target,field=self._gateway_prepared
            expected=self._input_value(field)
            bridge.before_enter()
            # The network acknowledgement can take time. Repeat native scope/draft checks after it.
            if self.window()!=target or self._input_value(field)!=expected:
                raise DesktopNotReady('Gateway fence acknowledged but target/draft changed; no Enter')
            self._image_rows(target)
            batch=getattr(self,'_gateway_batch_check',None)
            if batch:
                args,kwargs=batch
                if not super()._batch_draft_matches(*args,**kwargs):
                    raise DesktopNotReady('Original-image batch changed after gateway fence; no Enter')
            else:self._focus_text_input(target,field,expected)
        return super()._press(pid,code)
