module com.example.flink {
    requires javafx.controls;
    requires javafx.fxml;


    opens com.example.flink to javafx.fxml;
    exports com.example.flink;
}