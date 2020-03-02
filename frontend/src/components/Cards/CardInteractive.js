import React from "react";
import {
    Card,
    H5,
    Button
} from "@blueprintjs/core";

const CardInterative = (props) => {

    return (
      <div style={styles.cardStyle}>

          <Card elevation="3" interactive={true}>

              <H5>
                  {props.heading}
              </H5>

              <p>
                  {props.description}
              </p>

          </Card>

      </div>
    );
};

const styles = {
    cardStyle : {
        width: '100%'
    }
};

export default CardInterative;