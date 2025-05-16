///
/// `not_recoverable_error!` shall be used when the code is in position that it can only abort and there is no sense to return back to user
/// with the error. After calling this macro, control will never return to the user.
///
/// TODO: In future we shall provide probably some global hook that we call before aborting with panic! or with any other method.
///
#[macro_export]
macro_rules! not_recoverable_error {
    // handles a case where we want to handle Result<> error as non recoverable in form not_recoverable_error!(err RESULT, "MSG");
    ( on_err $result_obj:expr, $literal_str:expr ) => {{
        const MSG: &str = $literal_str;
        if ($result_obj).is_err() {
            let err = ($result_obj).unwrap_err();

            error!("not_recoverable_error: {} with error {:?}", $literal_str, err);
            panic!(
                "Currently no custom handler connected for panic, using rust one. Panicked with {} with {:?}",
                $literal_str, err
            );
        }
    }};
    // handles a case where we want to log the object with an error in form not_recoverable_error!(with OBJECT, "MSG");
    ( with $obj_to_log:expr, $literal_str:expr ) => {{
        const MSG: &str = $literal_str;
        error!("not_recoverable_error: {} with {:?}", $literal_str, $obj_to_log);
        panic!(
            "Currently no custom handler connected for panic, using rust one. Panicked with {} with {:?}",
            $literal_str, $obj_to_log
        );
    }};

    // handles a simple string literal error not_recoverable_error!("MSG");
    ( $literal_str:expr ) => {{
        const MSG: &str = $literal_str;
        error!("not_recoverable_error: {}", $literal_str);
        panic!(
            "Currently no custom handler connected for panic, using rust one. Panicked with {}",
            $literal_str
        );
    }};
}
